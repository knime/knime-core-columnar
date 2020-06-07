package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnSelection;

final class FilteredTableReadCursor implements TableReadCursor {

	private final ColumnDataReader m_reader;
	private final ColumnDataAccess<ColumnData>[] m_access;

	// filter
	private final int m_numChunks;
	private final long m_lastChunkMaxIndex;
	private final int[] m_selection;

	private int m_chunkIndex = 0;
	private int m_currentDataMaxIndex;
	private int m_index = -1;

	private ColumnData[] m_currentData;

	FilteredTableReadCursor(final ColumnDataReader reader, //
			final TableSchema schema, //
			final TableReadCursorConfig config) {
		m_reader = reader;
		m_access = createAccess(schema, config.getColumnSelection());

		m_selection = config.getColumnSelection().get();

		// starting with current chunk index. Iterating numChunks
		m_chunkIndex = (int) (config.getMinRowIndex() / reader.getMaxDataCapacity());
		m_numChunks = (int) Math.min(m_reader.getNumChunks(),
				(config.getMaxRowIndex() / reader.getMaxDataCapacity()) + 1);
		m_index = (int) (config.getMinRowIndex() % reader.getMaxDataCapacity()) - 1;
		m_lastChunkMaxIndex = config.getMaxRowIndex() % reader.getMaxDataCapacity();

		switchToNextData();

		for (final int i : m_selection) {
			m_access[i].setIndex(m_index);
		}
	}

	@Override
	public void fwd() {
		if (++m_index > m_currentDataMaxIndex) {
			switchToNextData();
			m_index = 0;
		}
		for (final int i : m_selection) {
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
		return m_index < m_currentDataMaxIndex || m_chunkIndex < m_numChunks;
	}

	@Override
	public void close() throws Exception {
		releaseCurrentData();
		m_reader.close();
	}

	private void switchToNextData() {
		try {
			releaseCurrentData();
			m_currentData = m_reader.read(m_chunkIndex++);
			for (final int i : m_selection) {
				m_access[i].load(m_currentData[i]);
			}

			// as soon as we're in the last chunk, we might want to iterate fewer values.
			if (m_chunkIndex == m_numChunks) {
				// TODO get rid of cast.
				m_currentDataMaxIndex = (int) m_lastChunkMaxIndex;
			} else {
				m_currentDataMaxIndex = m_currentData[0].getNumValues() - 1;
			}
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void releaseCurrentData() {
		if (m_currentData != null) {
			for (final int i : m_selection) {
				m_currentData[i].release();
			}
		}
	}

	private ColumnDataAccess<ColumnData>[] createAccess(TableSchema schema, ColumnSelection config) {
		@SuppressWarnings("unchecked")
		final ColumnDataAccess<? extends ColumnData>[] accesses = new ColumnDataAccess[schema.getNumColumns()];
		final int[] selectedColumns = config.get();
		for (int i = 0; i < selectedColumns.length; i++) {
			accesses[selectedColumns[i]] = schema.getColumnSpec(selectedColumns[i]).createAccess();
		}
		@SuppressWarnings("unchecked")
		final ColumnDataAccess<ColumnData>[] cast = (ColumnDataAccess<ColumnData>[]) accesses;
		return cast;
	}
}