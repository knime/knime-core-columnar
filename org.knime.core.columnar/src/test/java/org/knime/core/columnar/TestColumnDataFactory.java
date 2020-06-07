package org.knime.core.columnar;

import org.knime.core.columnar.chunk.ColumnDataFactory;

public class TestColumnDataFactory implements ColumnDataFactory {

	private final int m_numColumns;
	private final int m_chunkCapacity;

	public TestColumnDataFactory(int numColumns, int chunkCapacity) {
		m_numColumns = numColumns;
		m_chunkCapacity = chunkCapacity;
	}

	@Override
	public ColumnData[] create() {
		final ColumnData[] data = new ColumnData[m_numColumns];
		for (int i = 0; i < m_numColumns; i++) {
			data[i] = new TestDoubleColumnData(m_chunkCapacity);
			data[i].ensureCapacity(m_chunkCapacity);
		}
		return data;
	}

}
