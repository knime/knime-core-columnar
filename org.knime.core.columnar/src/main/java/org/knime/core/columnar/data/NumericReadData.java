package org.knime.core.columnar.data;

import org.knime.core.columnar.NullableColumnData;

public interface NumericReadData extends NullableColumnData {
	double getDouble(int index);
}
