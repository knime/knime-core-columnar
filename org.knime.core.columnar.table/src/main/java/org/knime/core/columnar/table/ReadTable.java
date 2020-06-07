package org.knime.core.columnar.table;

public interface ReadTable extends AutoCloseable {

	int getNumColumns();

	TableReadCursor cursor(TableReadFilter config);

	TableReadCursor cursor();

	TableSchema getSchema();
}
