package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.chunk.ColumnSelection;

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
	public TableReadCursor cursor(final TableReadFilter config) {
		if (config != null) {
			final ColumnSelection columnConfig = config.getColumnSelection();
			return new FilteredTableReadCursor(//
					m_delegate.createReader(columnConfig), //
					m_schema, //
					config);
		} else {
			return cursor();
		}
	}

	@Override
	public TableReadCursor cursor() {
		return new DefaultTableReadCursor(m_delegate.createReader(), m_schema);
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
}
