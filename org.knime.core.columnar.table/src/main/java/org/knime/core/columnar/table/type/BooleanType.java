package org.knime.core.columnar.table.type;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.BooleanData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class BooleanType implements ColumnType<BooleanData> {

	@Override
	public BooleanAccess createAccess() {
		return new BooleanAccess();
	}

	public static interface BooleanWriteValue extends NullableWriteValue {
		void setBoolean(boolean value);
	}

	public static interface BooleanReadValue extends NullableReadValue {
		boolean getBoolean();
	}

	static class BooleanAccess extends AbstractColumnDataAccess<BooleanData>
			implements BooleanReadValue, BooleanWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setBoolean(boolean value) {
			m_data.setBoolean(m_index, value);
		}

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public boolean getBoolean() {
			return m_data.getBoolean(m_index);
		}
	}

	@Override
	public ColumnDataSpec<BooleanData> getColumnDataSpec() {
		return new BooleanData.BooleanDataSpec();
	}
}
