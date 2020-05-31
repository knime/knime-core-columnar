package org.knime.core.columnar.table;

public interface TableReadCursor extends AutoCloseable{

	void fwd();

	<R extends ReadValue> R get(int index);

	boolean canFwd();

}
