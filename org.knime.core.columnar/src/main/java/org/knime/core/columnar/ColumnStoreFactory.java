package org.knime.core.columnar;

import java.io.File;

public interface ColumnStoreFactory {

	ColumnStore createWriteStore(ColumnStoreSchema schema, File file);

	ColumnReadStore createReadStore(ColumnStoreSchema schema, File file);

}
