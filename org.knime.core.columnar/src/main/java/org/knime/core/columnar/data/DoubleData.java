package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;

public interface DoubleData extends NumericReadData {
	double getDouble(int index);

	void setDouble(int index, double val);

	default DoubleDataSpec getColumnDataSpec() {
		return new DoubleDataSpec();
	}

	class DoubleDataSpec implements ColumnDataSpec<DoubleData> {
		@Override
		public Class<? extends DoubleData> getColumnDataType() {
			return DoubleData.class;
		}
	}
}