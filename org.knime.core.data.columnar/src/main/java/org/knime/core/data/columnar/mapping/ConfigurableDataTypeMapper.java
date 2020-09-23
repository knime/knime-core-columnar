
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.DataTypeConfig;
import org.knime.core.data.columnar.ColumnType;

public interface ConfigurableDataTypeMapper<T extends DataTypeConfig> extends DataTypeMapper {

	Class<T> getConfigClass();

	ColumnType<?, ?> createColumnType(T config);
}
