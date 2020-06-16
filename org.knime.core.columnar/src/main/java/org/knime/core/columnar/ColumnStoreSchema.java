package org.knime.core.columnar;

public interface ColumnStoreSchema {
	ColumnDataSpec<?> getColumnDataSpec(int index);

	int getNumColumns();
}
