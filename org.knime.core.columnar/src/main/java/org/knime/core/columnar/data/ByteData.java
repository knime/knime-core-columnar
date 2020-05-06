package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;

public interface ByteData extends NumericReadData {
	byte getByte(int index);

	void setByte(int index, byte val);

	class ByteDataSpec implements ColumnDataSpec<ByteData> {
		@Override
		public Class<? extends ByteData> getColumnDataType() {
			return ByteData.class;
		}
	}
}