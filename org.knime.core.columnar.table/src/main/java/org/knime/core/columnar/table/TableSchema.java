package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStoreSchema;

public interface TableSchema extends ColumnStoreSchema {

	ColumnType<?> getColumnSpec(int idx);

	int getNumColumns();

	@Override
	default ColumnDataSpec<?> getColumnDataSpec(int idx) {
		return getColumnSpec(idx).getColumnDataSpec();
	}
}
