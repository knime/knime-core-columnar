package org.knime.core.columnar.chunk;

import org.knime.core.columnar.ColumnData;

public interface ColumnDataFactory {
	ColumnData[] create();

	void setChunkSize(int chunkSize);
}
