package org.knime.core.columnar.table;

import org.knime.core.columnar.chunk.ColumnSelection;

public interface TableReadCursorConfig {
	ColumnSelection getColumnSelection();

	long getMinRowIndex();

	long getMaxRowIndex();
}
