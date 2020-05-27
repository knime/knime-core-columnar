package org.knime.core.columnar.cache;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnReaderConfig;

// not! thread-safe
public class InMemoryColumnStore implements ColumnStore, ReferencedData {

	private final ColumnStoreSchema m_schema;
	
	private final List<ColumnData[]> m_batches = new ArrayList<>();
	
	private final ColumnDataWriter m_writer = new ColumnDataWriter() {
		
		@Override
		public void write(ColumnData[] batch) throws IOException {
			for (ColumnData data : batch) {
				data.retain();
				m_sizeOf += data.sizeOf();
			}
			m_batches.add(batch);
		}
		
		@Override
		public void close() throws Exception {
		}
	};
	
	private int m_sizeOf = 0;

	InMemoryColumnStore(ColumnStoreSchema schema) {
		m_schema = schema;
	}

	@Override
	public void release() {
		for (ColumnData[] batch : m_batches) {
			for (ColumnData data : batch) {
				data.release();
			}
		}
	}

	@Override
	public void retain() {
		for (ColumnData[] batch : m_batches) {
			for (ColumnData data : batch) {
				data.retain();
			}
		}
	}

	@Override
	public int sizeOf() {
		return m_sizeOf;
	}

	@Override
	public ColumnDataWriter getWriter() {
		return m_writer;
	}

	@Override
	public void saveToFile(File f) throws IOException {
		throw new UnsupportedOperationException("Saving to file not supported by in-memory column store.");
	}

	@Override
	public ColumnDataReader createReader(ColumnReaderConfig config) {
		return new ColumnDataReader() {
			
			@Override
			public ColumnData[] read(int index) throws IOException {
				final int[] indices = config.getColumnIndices();
				final boolean isSelection = indices != null;
				final int numRequested = isSelection ? indices.length : m_schema.getNumColumns();
				final ColumnData[] batch = new ColumnData[numRequested];

				for (int i = 0; i < numRequested; i++) {
					final ColumnData data = m_batches.get(index)[isSelection ? indices[i] : i];
					data.retain();
					batch[i] = data;
				}

				return batch;
			}
			
			@Override
			public int getNumEntries() {
				return m_batches.size();
			}
			
			@Override
			public void close() throws Exception {
			}
		};
	}
	
	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}
	
	@Override
	public ColumnDataFactory getFactory() {
		throw new UnsupportedOperationException("Creating new ColumnData not supported by in-memory column store.");
	}

	@Override
	public void close() throws Exception {
		release();
	}
	
}
