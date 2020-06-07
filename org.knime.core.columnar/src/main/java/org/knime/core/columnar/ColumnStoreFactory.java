package org.knime.core.columnar;

import java.io.File;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz
 */
public interface ColumnStoreFactory {

	/**
	 * Creates a new {@link ColumnStore}.
	 * 
	 * @param the          physical {@link ColumnStoreSchema}
	 * @param file         to store data if out of memory
	 * @param minChunkSize the size of one chunk. NB: {@link ColumnStoreFactory} can
	 *                     adjust chunkSize.
	 * @return a {@link ColumnStore} with the given schema.
	 */
	ColumnStore createWriteStore(ColumnStoreSchema schema, File file, final int minChunkSize);

	/**
	 * Creates a new {@link ColumnReadStore}, reading data from the provided file.
	 * 
	 * @param schema creates a {@link ColumnReadStore}.
	 * @param file   from which data is read.
	 * @return the ColumnReadStore.
	 */
	ColumnReadStore createReadStore(ColumnStoreSchema schema, File file);

}
