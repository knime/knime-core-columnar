package org.knime.core.columnar.table;

import org.knime.core.columnar.chunk.ColumnSelection;

public interface TableReadFilter {
	ColumnSelection getColumnSelection();

	long getMinRowIndex();

	long getMaxRowIndex();
}
