package org.knime.core.columnar.cache;

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.CachedColumnReadStore.CachedColumnReadStoreCache;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnStore} that stores {@link ColumnData} in a fixed-size
 * {@link SmallColumnStoreCache LRU cache} in memory and passes it on to a
 * delegate column store. Once evicted from the cache, the data can only be read
 * from the delegate. The store allows concurrent reading via multiple
 * {@link ColumnDataReader ColumnDataReaders} once the {@link ColumnDataWriter}
 * has been closed.
 * 
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class CachedColumnStore implements ColumnStore {

	private final class CachedColumnStoreWriter implements ColumnDataWriter {

		// lazily initialized
		private ColumnDataWriter m_delegateWriter;

		@Override
		public void write(final ColumnData[] data) throws IOException {
			if (m_writerClosed) {
				throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
			}

			m_readCache.addBatch(data);
			m_numChunks.incrementAndGet();
			if (m_delegateWriter == null) {
				m_delegateWriter = m_delegate.getWriter();
			}
			m_delegateWriter.write(data);
		}

		@Override
		public void close() throws IOException {
			if (m_delegateWriter != null) {
				m_delegateWriter.close();
			}
			m_writerClosed = true;
		}
	}

	private final ColumnStore m_delegate;

	private final ColumnStoreSchema m_schema;

	private final CachedColumnReadStore m_readCache;

	private final AtomicInteger m_numChunks = new AtomicInteger();

	private final ColumnDataWriter m_writer;

	private volatile boolean m_writerClosed;

	private volatile boolean m_storeClosed;

	public CachedColumnStore(final ColumnStore delegate, final CachedColumnReadStoreCache cache) {
		m_delegate = delegate;
		m_schema = delegate.getSchema();
		m_readCache = new CachedColumnReadStore(delegate, cache);
		m_writer = new CachedColumnStoreWriter();
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

		m_delegate.saveToFile(file);
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection selection) {
		if (!m_writerClosed) {
			throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
		}

		return m_readCache.createReader(selection);
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
		m_writer.close();
		m_readCache.close();
		m_delegate.close();
		m_storeClosed = true;
	}
}
