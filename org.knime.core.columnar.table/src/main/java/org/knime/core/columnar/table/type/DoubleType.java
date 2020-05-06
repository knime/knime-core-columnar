package org.knime.core.columnar.table.type;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.DoubleData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class DoubleType implements ColumnType<DoubleData> {

	@Override
	public DoubleAccess createAccess() {
		return new DoubleAccess();
	}

	public static interface DoubleReadValue extends NullableReadValue {
		double getDouble();
	}

	public static interface DoubleWriteValue extends NullableWriteValue {
		void setDouble(double value);
	}

	static class DoubleAccess extends AbstractColumnDataAccess<DoubleData>
			implements DoubleReadValue, DoubleWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setDouble(double value) {
			m_data.setDouble(m_index, value);
		}

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public double getDouble() {
			return m_data.getDouble(m_index);
		}
	}

	@Override
	public ColumnDataSpec<DoubleData> getChunkSpec() {
		return new DoubleData.DoubleDataSpec();
	}

}
