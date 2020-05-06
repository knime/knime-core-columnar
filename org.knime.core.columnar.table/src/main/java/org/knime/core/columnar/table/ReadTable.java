package org.knime.core.columnar.table;

import org.knime.core.columnar.chunk.ColumnReaderConfig;

public interface ReadTable extends AutoCloseable {
	
	int getNumColumns();

	TableReadCursor newCursor(ColumnReaderConfig config);

	default TableReadCursor newCursor() {
		return newCursor(new ColumnReaderConfig() {

			@Override
			public int[] getColumnIndices() {
				return null;
			}
		});
	}

	TableSchema getSchema();
}
