package org.knime.core.columnar.table.type;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.FloatData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class FloatType implements ColumnType<FloatData> {

	@Override
	public ColumnDataSpec<FloatData> getColumnDataSpec() {
		return new FloatData.FloatDataSpec();
	}

	@Override
	public FloatAccess createAccess() {
		return new FloatAccess();
	}

	public static interface FloatReadValue extends NullableReadValue {
		float getFloat();
	}

	public static interface FloatWriteValue extends NullableWriteValue {

		void setFloat(float value);
	}

	static class FloatAccess extends AbstractColumnDataAccess<FloatData> implements FloatReadValue, FloatWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setFloat(float value) {
			m_data.setFloat(m_index, value);
		}

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public float getFloat() {
			return m_data.getFloat(m_index);
		}
	}
}
