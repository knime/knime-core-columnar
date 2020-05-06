package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnDataSpec;

public interface ColumnType<C extends ColumnData> {

	// inside
	ColumnDataSpec<C> getChunkSpec();

	// outside
	ColumnDataAccess<C> createAccess();

}
