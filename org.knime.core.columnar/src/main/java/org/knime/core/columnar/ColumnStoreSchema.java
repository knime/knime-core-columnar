package org.knime.core.columnar;

public interface ColumnStoreSchema {
	ColumnDataSpec<?> getColumnDataSpec(int idx);

	int getNumColumns();
}
