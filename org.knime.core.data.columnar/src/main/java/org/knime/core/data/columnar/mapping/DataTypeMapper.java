
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.DataType;
import org.knime.core.data.columnar.ColumnType;

public interface DataTypeMapper extends Comparable<DataTypeMapper> {

	DataType getDataType();

	int getVersion();

	ColumnType<?, ?> createColumnType();

	// TODO: remove Comparable. Use external Comparator instead. Cleaner.
	@Override
	default int compareTo(final DataTypeMapper o) {
		// sort desc
		return o.getVersion() - getVersion();
	}
}
