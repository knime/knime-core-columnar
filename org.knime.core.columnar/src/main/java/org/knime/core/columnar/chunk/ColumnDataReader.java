package org.knime.core.columnar.chunk;

import java.io.IOException;

import org.knime.core.columnar.ColumnData;

public interface ColumnDataReader extends AutoCloseable {

	ColumnData[] read(int rowBatchIndex) throws IOException;

	int getNumEntries();
}
