package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;

public interface LongData extends NumericReadData {
	long getLong(int index);

	void setLong(int index, long integer);

	public final class LongDataSpec implements ColumnDataSpec<LongData> {
		@Override
		public Class<? extends LongData> getColumnDataType() {
			return LongData.class;
		}
	}
}