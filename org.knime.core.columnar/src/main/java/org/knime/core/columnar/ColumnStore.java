package org.knime.core.columnar;

public interface ColumnStore extends ColumnWriteStore, ColumnReadStore, AutoCloseable {
	ColumnStoreSchema getSchema();
}
