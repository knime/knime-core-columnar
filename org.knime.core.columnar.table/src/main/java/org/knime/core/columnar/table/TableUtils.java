package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.ColumnStore;

public final class TableUtils {
	private TableUtils() {
	}

	// TODO check compatibility of schema
	public static ReadTable createReadTable(TableSchema schema, ColumnReadStore store) {
		return new ColumnStoreReadTable(schema, store);
	}

	// TODO check compatibility of schema
	public static WriteTable createWriteTable(TableSchema schema, ColumnStore store) {
		return new ColumnStoreWriteTable(schema, store);
	}
}
