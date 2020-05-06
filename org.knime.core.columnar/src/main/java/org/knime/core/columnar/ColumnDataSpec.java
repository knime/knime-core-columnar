package org.knime.core.columnar;

public interface ColumnDataSpec<C extends ColumnData> {
	Class<? extends C> getColumnDataType();
}
