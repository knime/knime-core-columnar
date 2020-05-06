package org.knime.core.columnar.arrow;

import org.knime.core.columnar.ColumnStoreSchema;

public interface ArrowSchemaMapper {

	/**
	 * NB: Individual specs can have a state!
	 * 
	 * @param schema
	 * @return
	 */
	ArrowColumnDataSpec<?>[] map(ColumnStoreSchema schema);

}
