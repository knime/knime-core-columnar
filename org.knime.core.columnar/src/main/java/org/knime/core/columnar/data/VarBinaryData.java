package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface VarBinaryData extends NullableColumnData {
	void setBytes(int index, byte[] data);

	byte[] getBytes(int index);

	public class VarBinaryDataSpec implements ColumnDataSpec<VarBinaryData> {
		@Override
		public Class<? extends VarBinaryData> getColumnDataType() {
			return VarBinaryData.class;
		}
	}
}