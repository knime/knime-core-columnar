package org.knime.core.columnar.arrow;

import java.io.File;

import org.knime.core.columnar.ColumnStoreFactory;
import org.knime.core.columnar.ColumnStoreSchema;

public class ArrowColumnStoreFactory implements ColumnStoreFactory {

	@Override
	public ArrowColumnStore createWriteStore(ColumnStoreSchema schema, File file, int chunkSize) {
		// TODO make this part of the create method?
		return new ArrowColumnStore(schema, new ArrowSchemaMapperV0(), file, chunkSize);
	}

	@Override
	public ArrowColumnReadStore createReadStore(ColumnStoreSchema schema, File file) {
		return new ArrowColumnReadStore(schema, file);
	}
}
