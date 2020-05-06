package org.knime.core.columnar;

import java.io.File;
import java.io.IOException;

import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataWriter;

public interface ColumnWriteStore {
	ColumnDataWriter getWriter();

	ColumnDataFactory getFactory();

	void saveToFile(File f) throws IOException;
}
