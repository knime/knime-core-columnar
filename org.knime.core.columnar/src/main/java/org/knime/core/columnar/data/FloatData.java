package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;

public interface FloatData extends NumericReadData {
	float getFloat(int index);

	void setFloat(int index, float val);

	class FloatDataSpec implements ColumnDataSpec<FloatData> {
		@Override
		public Class<? extends FloatData> getColumnDataType() {
			return FloatData.class;
		}
	}
}
