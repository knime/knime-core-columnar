package org.knime.core.columnar.table.type;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.VarBinaryData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class VarBinaryType implements ColumnType<VarBinaryData> {

	@Override
	public ColumnDataSpec<VarBinaryData> getColumnDataSpec() {
		return new VarBinaryData.VarBinaryDataSpec();
	}

	@Override
	public VarBinaryAccess createAccess() {
		return new VarBinaryAccess();
	}

	public static interface BinaryReadValue extends NullableReadValue {
		byte[] getByteArray();
	}

	public static interface BinaryWriteValue extends NullableWriteValue {
		void setByteArray(byte[] array);
	}

	public static class VarBinaryAccess extends AbstractColumnDataAccess<VarBinaryData>
			implements BinaryReadValue, BinaryWriteValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setByteArray(byte[] array) {
			m_data.setBytes(m_index, array);
		}

		@Override
		public byte[] getByteArray() {
			return m_data.getBytes(m_index);
		}

	}

}