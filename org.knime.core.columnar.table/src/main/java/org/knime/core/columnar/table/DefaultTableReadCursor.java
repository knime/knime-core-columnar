package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.chunk.ColumnDataReader;

final class DefaultTableReadCursor implements TableReadCursor {

	private final ColumnDataReader m_reader;
	private final ColumnDataAccess<ColumnData>[] m_access;
	private final int m_numChunks;

	private int m_dataIndex = 0;
	private int m_currentDataMaxIndex;
	private int m_index = -1;

	private ColumnData[] m_currentData;

	DefaultTableReadCursor(final ColumnDataReader reader, TableSchema schema) {
		m_reader = reader;
		m_access = createAccess(schema);
		m_numChunks = m_reader.getNumChunks();

		switchToNextData();
	}

	@Override
	public void fwd() {
		if (++m_index > m_currentDataMaxIndex) {
			switchToNextData();
			m_index = 0;
		}
		for (int i = 0; i < m_access.length; i++) {
			m_access[i].fwd();
		}
	}

	@Override
	public <R extends ReadValue> R get(int index) {
		@SuppressWarnings("unchecked")
		final R cast = (R) m_access[index];
		return cast;
	}

	@Override
	public boolean canFwd() {
		return m_index < m_currentDataMaxIndex || m_dataIndex < m_numChunks;
	}

	private void switchToNextData() {
		try {
			releaseCurrentData();
			m_currentData = m_reader.read(m_dataIndex++);
			for (int i = 0; i < m_access.length; i++) {
				m_access[i].load(m_currentData[i]);
			}
			m_currentDataMaxIndex = m_currentData[0].getNumValues() - 1;
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws Exception {
		releaseCurrentData();
		m_reader.close();
	}

	private void releaseCurrentData() {
		if (m_currentData != null) {
			for (final ColumnData data : m_currentData) {
				data.release();
			}
		}
	}

	private ColumnDataAccess<ColumnData>[] createAccess(TableSchema schema) {
		@SuppressWarnings("unchecked")
		final ColumnDataAccess<? extends ColumnData>[] accesses = new ColumnDataAccess[schema.getNumColumns()];
		for (int i = 0; i < accesses.length; i++) {
			accesses[i] = schema.getColumnSpec(i).createAccess();
		}
		@SuppressWarnings("unchecked")
		final ColumnDataAccess<ColumnData>[] cast = (ColumnDataAccess<ColumnData>[]) accesses;
		return cast;
	}
}