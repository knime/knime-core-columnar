package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface BooleanData extends NullableColumnData {
	boolean getBoolean(int index);

	void setBoolean(int index, boolean val);

	public class BooleanDataSpec implements ColumnDataSpec<BooleanData> {
		@Override
		public Class<? extends BooleanData> getColumnDataType() {
			return BooleanData.class;
		}
	}
}
