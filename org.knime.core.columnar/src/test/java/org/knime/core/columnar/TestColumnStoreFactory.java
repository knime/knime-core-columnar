package org.knime.core.columnar;

import java.io.File;

public class TestColumnStoreFactory implements ColumnStoreFactory {

	@Override
	public ColumnStore createWriteStore(ColumnStoreSchema schema, File file, int chunkCapacity) {
		return new TestColumnStore(schema, chunkCapacity);
	}

	@Override
	public ColumnReadStore createReadStore(ColumnStoreSchema schema, File file) {
		throw new UnsupportedOperationException("not implemented");
	}
}
