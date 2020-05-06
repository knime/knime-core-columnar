package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;

class ColumnStoreWriteTable implements WriteTable, AutoCloseable {

	private final TableSchema m_schema;
	private final ColumnStore m_delegate;
	private TableWriteCursor m_cursor;

	// state dependent.
	private boolean m_closedForWriting;
	private ReadTable m_readTable;

	public ColumnStoreWriteTable(TableSchema schema, ColumnStore store) {
		m_delegate = store;
		m_schema = schema;
	}

	@Override
	public TableWriteCursor getCursor() {
		if (m_closedForWriting) {
			throw new IllegalStateException("ColumnStoreWriteTable closed for writing.");
		}

		if (m_cursor == null) {
			m_cursor = new TableWriteCursor(m_delegate.getFactory(), m_delegate.getWriter(), createAccess(m_schema));
		}
		return m_cursor;
	}

	@Override
	public TableSchema getSchema() {
		return m_schema;
	}

	@Override
	public int getNumColumns() {
		return m_schema.getNumColumns();
	}

	private static ColumnDataAccess<ColumnData>[] createAccess(TableSchema schema) {
		@SuppressWarnings("unchecked")
		final ColumnDataAccess<? extends ColumnData>[] accesses = new ColumnDataAccess[schema.getNumColumns()];
		for (int i = 0; i < accesses.length; i++) {
			accesses[i] = schema.getColumnSpec(i).createAccess();
		}
		@SuppressWarnings("unchecked")
		final ColumnDataAccess<ColumnData>[] cast = (ColumnDataAccess<ColumnData>[]) accesses;
		return cast;
	}

	@Override
	public void close() throws Exception {
		m_delegate.close();
	}

	@Override
	public ReadTable closeForWriting() {
		m_closedForWriting = true;
		if (m_readTable == null) {
			m_readTable = new ColumnStoreReadTable(m_schema, m_delegate);
		}
		return m_readTable;
	}

}
