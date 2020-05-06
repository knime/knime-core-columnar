package org.knime.core.columnar.chunk;

import java.io.IOException;

import org.knime.core.columnar.ColumnData;

public interface ColumnDataWriter extends AutoCloseable {
	void write(final ColumnData[] record) throws IOException;
}
