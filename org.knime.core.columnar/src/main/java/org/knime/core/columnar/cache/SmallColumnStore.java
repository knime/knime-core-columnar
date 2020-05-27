package org.knime.core.columnar.cache;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnReaderConfig;

//TODO: thread safety considerations
public class SmallColumnStore implements ColumnStore {

	private final ColumnStore m_delegate;

	private final ColumnStoreSchema m_schema;

	private final AtomicInteger m_numChunks = new AtomicInteger();

	private final int m_smallTableThreshold;

	private final LoadingEvictingChunkCache<SmallColumnStore, InMemoryColumnStore> m_cache;

	private final ColumnDataWriter m_writer;

	private volatile boolean m_writerClosed;

	private final AtomicBoolean m_isFlushed = new AtomicBoolean();

	public SmallColumnStore(ColumnStore delegate, int smallTableThreshold, int cacheSize) {
		m_delegate = delegate;
		m_schema = delegate.getSchema();
		m_cache = new SizeBoundLruCache<>(cacheSize);
		m_smallTableThreshold = smallTableThreshold;

		m_writer = new ColumnDataWriter() {

			private InMemoryColumnStore m_table = new InMemoryColumnStore(m_schema);

			// lazily initialized
			private ColumnDataWriter m_delegateWriter;

			@Override
			public void write(ColumnData[] batch) throws IOException {
				if (m_writerClosed) {
					throw new IllegalStateException("Table store writer has already been closed.");
				}

				m_numChunks.incrementAndGet();

				int sizeOf = 0;
				for (ColumnData data : batch) {
					sizeOf += data.sizeOf();
				}
				if (m_table != null) {
					if (m_table.sizeOf() + sizeOf <= m_smallTableThreshold) {
						for (ColumnData data : batch) {
							data.retain();
						}
						m_table.getWriter().write(batch);
						return;
					} else {
						try (ColumnDataReader reader = m_table.createReader(() -> null)) {
							for (int i = 0; i < reader.getNumEntries(); i++) {
								initAndWrite(reader.read(i));
							}
							m_table.close();
						} catch (Exception e) {
							// TODO: revisit error handling
							throw new IOException("Error while flushing small table.", e);
						}
						m_table = null;
					}
				}
				initAndWrite(batch);
			}

			private void initAndWrite(ColumnData[] batch) throws IOException {
				if (m_delegateWriter == null) {
					m_delegateWriter = m_delegate.getWriter();
				}
				m_delegateWriter.write(batch);
			}

			@Override
			public void close() throws Exception {
				if (m_table != null) {
					m_cache.retainAndPutIfAbsent(SmallColumnStore.this, m_table, (store, table) -> {
						synchronized (m_isFlushed) {
							if (m_isFlushed.compareAndSet(false, true)) {
								try (ColumnDataWriter delegateWriter = m_delegate.getWriter();
										ColumnDataReader reader = m_table.createReader(() -> null)) {
									for (int i = 0; i < reader.getNumEntries(); i++) {
										delegateWriter.write(reader.read(i));
									}
								} catch (Exception e) {
									// TODO: revisit error handling
									throw new RuntimeException("Error while flushing small table.", e);
								}
							}
						}
					});
					m_table.release();
				} else if (m_delegateWriter != null) {
					m_delegateWriter.close();
					m_isFlushed.set(true);
				}
				m_writerClosed = true;
			}

		};
	}

	@Override
	public ColumnDataWriter getWriter() {
		return m_writer;
	}

	@Override
	public void saveToFile(File file) throws IOException {
		if (!m_writerClosed) {
			throw new IllegalStateException("Table store writer has not been closed.");
		}

		synchronized (m_isFlushed) {
			if (m_isFlushed.compareAndSet(false, true)) {
				final InMemoryColumnStore cached = m_cache.retainAndGet(SmallColumnStore.this);
				if (cached != null) {
					try (ColumnDataWriter delegateWriter = m_delegate.getWriter();
							ColumnDataReader reader = cached.createReader(() -> null)) {
						for (int i = 0; i < reader.getNumEntries(); i++) {
							delegateWriter.write(reader.read(i));
						}
					} catch (Exception e) {
						throw new IOException("Error while flushing small table.", e);
					}
					cached.release();
				}
			}
		}
		m_delegate.saveToFile(file);
	}

	@Override
	public ColumnDataReader createReader(ColumnReaderConfig config) {
		if (!m_writerClosed) {
			throw new IllegalStateException("Table store writer has not been closed.");
		}

		return new ColumnDataReader() {

			// lazily initialized
			private ColumnDataReader m_delegateReader;

			@Override
			public ColumnData[] read(int chunkIndex) throws IOException {
				if (chunkIndex == 0) {
					final InMemoryColumnStore cached = m_cache.retainAndGet(SmallColumnStore.this);
					if (cached != null) {
						try (ColumnDataReader reader = cached.createReader(config)) {
							return reader.read(chunkIndex);
						} catch (Exception e) {
							throw new IOException("Error while reading from small table.", e);
						}
					}
				}
				if (m_delegateReader == null) {
					m_delegateReader = m_delegate.createReader(config);
				}
				return m_delegateReader.read(chunkIndex);
			}

			@Override
			public void close() throws Exception {
				if (m_delegateReader != null) {
					m_delegateReader.close();
				}
			}

			@Override
			public int getNumEntries() {
				return m_numChunks.get();
			}
		};
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

	@Override
	public ColumnDataFactory getFactory() {
		return m_delegate.getFactory();
	}

	@Override
	public void close() throws Exception {
		final InMemoryColumnStore removed = m_cache.remove(SmallColumnStore.this);
		if (removed != null) {
			removed.release();
		}
		m_writer.close();
		m_delegate.close();
	}

}
