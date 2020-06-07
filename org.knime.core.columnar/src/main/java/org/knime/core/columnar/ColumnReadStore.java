package org.knime.core.columnar;

import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnSelection;

public interface ColumnReadStore extends AutoCloseable {
	ColumnDataReader createReader(ColumnSelection config);
	
	default ColumnDataReader createReader() {
		return createReader(null);
	}

	ColumnStoreSchema getSchema();
}
