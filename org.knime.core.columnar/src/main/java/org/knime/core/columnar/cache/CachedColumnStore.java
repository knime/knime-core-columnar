package org.knime.core.columnar.cache;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnReaderConfig;

//TODO: thread safety considerations
public final class CachedColumnStore implements ColumnStore {
	
	private final ColumnStore m_delegate;

	private final ColumnStoreSchema m_schema;
	
	private final CachedColumnReadStore m_readCache;
	
	private final AtomicInteger m_numChunks = new AtomicInteger();
	
	private final ColumnDataWriter m_writer = new ColumnDataWriter() {
		
		// lazily initialized
		private ColumnDataWriter m_delegateWriter;
		
		@Override
		public void write(final ColumnData[] data) throws IOException {
			if (m_writerClosed) {
				throw new IllegalStateException("Attempting to write with an already closed writer.");
			}

			m_readCache.addBatch(data);
			m_numChunks.incrementAndGet();
			if (m_delegateWriter == null) {
				m_delegateWriter = m_delegate.getWriter();
			}
			m_delegateWriter.write(data);
		}

		@Override
		public void close() throws Exception {
			m_delegateWriter.close();
			m_writerClosed = true;
		}
	};
	
	private volatile boolean m_writerClosed;
	
	public CachedColumnStore(final ColumnStore delegate, final int cacheSize) {
		m_delegate = delegate;
		m_schema = delegate.getSchema();
		m_readCache = new CachedColumnReadStore(delegate, cacheSize);
	}

	@Override
	public ColumnDataWriter getWriter() {
		return m_writer;
	}
	
	@Override
	public void saveToFile(File file) throws IOException {
		m_delegate.saveToFile(file);
	}
	
	@Override
	public ColumnDataReader createReader(ColumnReaderConfig config) {
		return m_readCache.createReader(config);
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
		m_writer.close();
		m_readCache.close();
		m_delegate.close();
	}
	
}
