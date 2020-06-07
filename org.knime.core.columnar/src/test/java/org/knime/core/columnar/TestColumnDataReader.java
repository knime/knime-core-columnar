package org.knime.core.columnar;

import java.io.IOException;
import java.util.List;

import org.knime.core.columnar.chunk.ColumnDataReader;

class TestColumnDataReader implements ColumnDataReader {

	public boolean m_closed = false;

	private List<Double[][]> m_chunks;

	private int m_chunkCapacity;

	TestColumnDataReader(List<Double[][]> chunks, final int chunkCapacity) {
		m_chunks = chunks;
		m_chunkCapacity = chunkCapacity;
	}

	@Override
	public void close() throws Exception {
		m_closed = true;
	}

	@Override
	public ColumnData[] read(int chunkIndex) throws IOException {
		final Double[][] data = m_chunks.get(chunkIndex);
		final TestDoubleColumnData[] columnData = new TestDoubleColumnData[data.length];
		for (int i = 0; i < data.length; i++) {
			columnData[i] = new TestDoubleColumnData(data[i]);
		}
		return columnData;
	}

	@Override
	public int getNumChunks() {
		return m_chunks.size();
	}

	@Override
	public int getMaxDataCapacity() {
		return m_chunkCapacity;
	}

}
