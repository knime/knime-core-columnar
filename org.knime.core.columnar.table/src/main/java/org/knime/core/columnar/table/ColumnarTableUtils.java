package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.ColumnStore;

public final class ColumnarTableUtils {
	private ColumnarTableUtils() {
	}

	// TODO check compatibility of schema
	public static ColumnStoreReadTable createReadTable(TableSchema schema, ColumnReadStore store) {
		return new ColumnStoreReadTable(schema, store);
	}

	// TODO check compatibility of schema
	public static ColumnStoreWriteTable createWriteTable(TableSchema schema, ColumnStore store) {
		return new ColumnStoreWriteTable(schema, store);
	}
}
