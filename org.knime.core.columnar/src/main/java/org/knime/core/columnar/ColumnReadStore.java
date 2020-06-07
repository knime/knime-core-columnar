package org.knime.core.columnar;

import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnReaderConfig;

public interface ColumnReadStore extends AutoCloseable {
	ColumnDataReader createReader(ColumnReaderConfig config);
	
	default ColumnDataReader createReader() {
		return createReader(null);
	}

	ColumnStoreSchema getSchema();
}
