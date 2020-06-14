package org.knime.core.columnar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

public class TestColumnStore implements ColumnStore {

	private final int m_maxDataCapacity;
	private final ColumnStoreSchema m_schema;

	public List<Double[][]> m_chunks;

	private List<TestDoubleColumnData[]> m_tracker;

	public TestColumnStore(ColumnStoreSchema schema, int maxDataCapacity) {
		m_schema = schema;
		m_maxDataCapacity = maxDataCapacity;
		m_chunks = new ArrayList<Double[][]>();
		m_tracker = new ArrayList<TestDoubleColumnData[]>();
	}

	@Override
	public ColumnDataWriter getWriter() {
		return new TestColumnDataWriter(m_chunks);
	}

	@Override
	public ColumnDataFactory getFactory() {
		return new TestColumnDataFactory(m_schema.getNumColumns(), m_maxDataCapacity);
	}

	@Override
	public void saveToFile(File f) throws IOException {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ColumnDataReader createReader(ColumnSelection config) {
		final TestColumnDataReader reader = new TestColumnDataReader(m_chunks, m_maxDataCapacity);
		return new ColumnDataReader() {

			@Override
			public void close() throws Exception {
				reader.close();
			}

			@Override
			public ColumnData[] read(int chunkIndex) throws IOException {
				final ColumnData[] data = reader.read(chunkIndex);
				m_tracker.add((TestDoubleColumnData[]) data);
				return data;
			}

			@Override
			public int getNumChunks() {
				return reader.getNumChunks();
			}

			@Override
			public int getMaxDataCapacity() {
				return reader.getMaxDataCapacity();
			}
		};
	}

	@Override
	public void close() throws Exception {
		m_chunks = null;

		// check if all memory has been released before closing this store.
		for (TestDoubleColumnData[] chunk : m_tracker) {
			for (TestDoubleColumnData data : chunk) {
				if (!data.isClosed()) {
					throw new IllegalStateException("Data not closed.");
				}
			}
		}
	}

	@Override
	public ColumnStoreSchema getSchema() {
		return m_schema;
	}

}
