package org.knime.core.columnar.table.type;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.ByteData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class ByteType implements ColumnType<ByteData> {

	@Override
	public ByteAccess createAccess() {
		return new ByteAccess();
	}

	@Override
	public ColumnDataSpec<ByteData> getColumnDataSpec() {
		return new ByteData.ByteDataSpec();
	}

	public static interface ByteWriteValue extends NullableWriteValue {
		void setByte(byte value);
	}

	public static interface ByteReadValue extends NullableReadValue {
		byte getByte();
	}

	public static class ByteAccess extends AbstractColumnDataAccess<ByteData> implements ByteReadValue, ByteWriteValue {
		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public void setByte(byte value) {
			m_data.setByte(m_index, value);
		}

		@Override
		public byte getByte() {
			return m_data.getByte(m_index);
		}
	}
}
