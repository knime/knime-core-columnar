package org.knime.core.columnar.table.type;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.IntData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class IntType implements ColumnType<IntData> {

	@Override
	public ColumnDataSpec<IntData> getColumnDataSpec() {
		return new IntData.IntDataSpec();
	}

	@Override
	public IntAccess createAccess() {
		return new IntAccess();
	}

	public static interface IntReadValue extends NullableReadValue {
		int getIntValue();
	}

	public static interface IntWriteValue extends NullableWriteValue {
		void setIntValue(int value);
	}

	public static class IntAccess extends AbstractColumnDataAccess<IntData> implements IntReadValue, IntWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setIntValue(int value) {
			m_data.setInt(m_index, value);
		}

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public int getIntValue() {
			return m_data.getInt(m_index);
		}
	}

}
