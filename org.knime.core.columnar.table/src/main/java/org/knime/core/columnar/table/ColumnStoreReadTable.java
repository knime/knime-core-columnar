package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.chunk.ColumnReaderConfig;

public final class ColumnStoreReadTable implements ReadTable {

	private final TableSchema m_schema;
	private final ColumnReadStore m_delegate;

	ColumnStoreReadTable(TableSchema schema, ColumnReadStore delegate) {
		m_schema = schema;
		m_delegate = delegate;
	}

	@Override
	public TableSchema getSchema() {
		return m_schema;
	}

	@Override
	public TableReadCursor newCursor(ColumnReaderConfig config) {
		return new TableReadCursor(m_delegate.createReader(config), createAccess(m_schema, config));
	}

	@Override
	public void close() throws Exception {
		m_delegate.close();
	}

	@Override
	public int getNumColumns() {
		return m_schema.getNumColumns();
	}

	public ColumnReadStore getStore() {
		return m_delegate;
	}

	private ColumnDataAccess<ColumnData>[] createAccess(TableSchema schema, ColumnReaderConfig config) {
		@SuppressWarnings("unchecked")
		final ColumnDataAccess<? extends ColumnData>[] accesses = new ColumnDataAccess[schema.getNumColumns()];
		if (config.getColumnIndices() == null) {
			for (int i = 0; i < accesses.length; i++) {
				accesses[i] = schema.getColumnSpec(i).createAccess();
			}
		} else {
			final int[] selectedColumns = config.getColumnIndices();
			for (int i = 0; i < selectedColumns.length; i++) {
				accesses[selectedColumns[i]] = schema.getColumnSpec(selectedColumns[i]).createAccess();
			}
		}
		@SuppressWarnings("unchecked")
		final ColumnDataAccess<ColumnData>[] cast = (ColumnDataAccess<ColumnData>[]) accesses;
		return cast;
	}

}
