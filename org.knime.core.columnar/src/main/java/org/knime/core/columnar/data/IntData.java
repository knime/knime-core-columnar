package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;

public interface IntData extends NumericReadData {
	int getInt(int index);

	void setInt(int index, int integer);

	public class IntDataSpec implements ColumnDataSpec<IntData> {
		@Override
		public Class<? extends IntData> getColumnDataType() {
			return IntData.class;
		}
	}
}
