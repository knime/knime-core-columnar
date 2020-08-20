package org.knime.core.columnar.cache;

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnStore} that stores {@link ColumnData} in a fixed-size
 * {@link SmallColumnStoreCache LRU cache} in memory if the aggregated
 * {@link ReferencedData#sizeOf() size} of data is below a given threshold. If
 * the threshold is exceeded or once evicted from the cache, the data is passed
 * on to a delegate column store. The store allows concurrent reading via
 * multiple {@link ColumnDataReader ColumnDataReaders} once the
 * {@link ColumnDataWriter} has been closed.
 * 
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class SmallColumnStore implements ColumnStore {

	private static final String ERROR_MESSAGE_ON_FLUSH = "Error while flushing small table.";

	private static final String ERROR_MESSAGE_ON_READ = "Error while reading small table.";

	public static final class SmallColumnStoreCache {

		private final int m_smallTableThreshold;

		private final LoadingEvictingCache<SmallColumnStore, InMemoryColumnStore> m_cache;

		public SmallColumnStoreCache(int smallTableThreshold, long cacheSize) {
			m_smallTableThreshold = smallTableThreshold;
			m_cache = new SizeBoundLruCache<>(cacheSize);
		}

		int size() {
			return m_cache.size();
		}
	}

	private final class SmallColumnStoreWriter implements ColumnDataWriter {

		private InMemoryColumnStore m_table = new InMemoryColumnStore(m_schema);

		// lazily initialized
		private ColumnDataWriter m_delegateWriter;

		@Override
		public void write(ColumnData[] batch) throws IOException {
			if (m_writerClosed) {
				throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
			}

			m_numChunks.incrementAndGet();

			if (m_table != null) {
				m_table.getWriter().write(batch);
				if (m_table.sizeOf() > m_smallTableThreshold) {
					try {
						m_table.getWriter().close();
						try (ColumnDataReader reader = m_table.createReader()) {
							for (int i = 0; i < reader.getNumChunks(); i++) {
								ColumnData[] cached = reader.read(i);
								initAndWrite(cached);
								for (ColumnData data : cached) {
									data.release();
								}
							}
							m_table.close();
						}
					} catch (Exception e) {
						throw new IOException(ERROR_MESSAGE_ON_FLUSH, e);
					}
					m_table = null;
				}
			} else {
				initAndWrite(batch);
			}
		}

		private void initAndWrite(ColumnData[] batch) throws IOException {
			if (m_delegateWriter == null) {
				m_delegateWriter = m_delegate.getWriter();
			}
			m_delegateWriter.write(batch);
		}

		@Override
		public void close() throws IOException {
			if (m_table != null) {
				m_table.getWriter().close();
				m_cache.retainAndPutIfAbsent(SmallColumnStore.this, m_table, (store, table) -> {
					synchronized (m_isFlushed) {
						if (m_isFlushed.compareAndSet(false, true)) {
							try (ColumnDataWriter delegateWriter = m_delegate.getWriter();
									ColumnDataReader reader = table.createReader()) {
								for (int i = 0; i < reader.getNumChunks(); i++) {
									delegateWriter.write(reader.read(i));
								}
								table.close();
							} catch (Exception e) {
								throw new IllegalStateException(ERROR_MESSAGE_ON_FLUSH, e);
							}
						}
					}
				});
				m_table.release(); // from here on out, the cache is responsible for retaining
				m_table = null;
			} else if (m_delegateWriter != null) {
				m_delegateWriter.close();
				m_isFlushed.set(true);
				m_delegateWriter = null;
			}
			m_writerClosed = true;
		}
	}

	private final class SmallColumnStoreReader implements ColumnDataReader {

		private final ColumnSelection m_selection;

		// lazily initialized
		private ColumnDataReader m_delegateReader;

		SmallColumnStoreReader(ColumnSelection selection) {
			m_selection = selection;
		}

		@Override
		public ColumnData[] read(int chunkIndex) throws IOException {
			if (m_storeClosed) {
				throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
			}

			final InMemoryColumnStore cached = m_cache.retainAndGet(SmallColumnStore.this);
			if (cached != null) {
				try (ColumnDataReader reader = cached.createReader(m_selection)) {
					final ColumnData[] batch = reader.read(chunkIndex);
					cached.release();
					return batch;
				} catch (Exception e) {
					throw new IOException(ERROR_MESSAGE_ON_READ, e);
				}
			}
			if (m_delegateReader == null) {
				m_delegateReader = m_delegate.createReader(m_selection);
			}
			return m_delegateReader.read(chunkIndex);
		}

		@Override
		public void close() throws IOException {
			if (m_delegateReader != null) {
				m_delegateReader.close();
			}
		}

		@Override
		public int getNumChunks() {
			return m_numChunks.get();
		}

		@Override
		public int getMaxDataCapacity() {
			return m_delegateReader.getMaxDataCapacity();
		}
	}

	private final ColumnStore m_delegate;

	private final ColumnStoreSchema m_schema;

	private final AtomicInteger m_numChunks = new AtomicInteger();

	private final int m_smallTableThreshold;

	private final LoadingEvictingCache<SmallColumnStore, InMemoryColumnStore> m_cache;

	private final ColumnDataWriter m_writer;
	
	private final AtomicBoolean m_isFlushed = new AtomicBoolean();

	private volatile boolean m_writerClosed;

	private volatile boolean m_storeClosed;

	public SmallColumnStore(ColumnStore delegate, SmallColumnStoreCache cache) {
		m_delegate = delegate;
		m_schema = delegate.getSchema();
		m_cache = cache.m_cache;
		m_smallTableThreshold = cache.m_smallTableThreshold;
		m_writer = new SmallColumnStoreWriter();
	}

	@Override
	public ColumnDataWriter getWriter() {
		return m_writer;
	}

	@Override
	public void saveToFile(File file) throws IOException {
		if (!m_writerClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
		}
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		synchronized (m_isFlushed) {
			if (m_isFlushed.compareAndSet(false, true)) {
				final InMemoryColumnStore cached = m_cache.retainAndGet(SmallColumnStore.this);
				if (cached != null) {
					try (ColumnDataWriter delegateWriter = m_delegate.getWriter();
							ColumnDataReader reader = cached.createReader()) {
						for (int i = 0; i < reader.getNumChunks(); i++) {
							ColumnData[] batch = reader.read(i);
							delegateWriter.write(batch);
							for (ColumnData data : batch) {
								data.release();
							}
						}
					} catch (Exception e) {
						throw new IOException(ERROR_MESSAGE_ON_FLUSH, e);
					}
					cached.release();
				}
			}
		}
		m_delegate.saveToFile(file);
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection selection) {
		if (!m_writerClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
		}
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		return new SmallColumnStoreReader(selection);
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

	@Override
	public ColumnDataFactory getFactory() {
		if (m_storeClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
		}

		return m_delegate.getFactory();
	}

	@Override
	public void close() throws IOException {
		final InMemoryColumnStore removed = m_cache.remove(SmallColumnStore.this);
		if (removed != null) {
			removed.release();
		}
		m_writer.close();
		m_delegate.close();
		m_storeClosed = true;
	}
}
