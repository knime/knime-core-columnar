package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;

public class ColumnStoreWriteTable implements WriteTable {

	private final TableSchema m_schema;
	private final ColumnStore m_store;
	private TableWriteCursor m_cursor;

	// state dependent.
	private ColumnStoreReadTable m_readTable;

	ColumnStoreWriteTable(TableSchema schema, ColumnStore store) {
		m_store = store;
		m_schema = schema;
	}

	public ColumnStore getStore() {
		return m_store;
	}

	@Override
	public TableWriteCursor getCursor() {
		if (m_readTable != null) {
			throw new IllegalStateException("ColumnStoreWriteTable closed for writing.");
		}

		if (m_cursor == null) {
			m_cursor = new TableWriteCursor(m_store.getFactory(), m_store.getWriter(), createAccess(m_schema));
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
		m_store.close();
	}

	@Override
	public ColumnStoreReadTable createReadTable() throws Exception {
		if (m_cursor != null) {
			m_cursor.close();
			m_cursor = null;
		}
		if (m_readTable == null) {
			m_readTable = new ColumnStoreReadTable(m_schema, m_store);
		}

		return m_readTable;
	}
}
