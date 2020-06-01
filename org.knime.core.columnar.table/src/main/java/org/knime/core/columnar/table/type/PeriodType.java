package org.knime.core.columnar.table.type;

import java.time.Period;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.PeriodData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class PeriodType implements ColumnType<PeriodData> {

	@Override
	public ColumnDataSpec<PeriodData> getColumnDataSpec() {
		return new PeriodData.PeriodDataSpec();
	}

	@Override
	public PeriodAccess createAccess() {
		return new PeriodAccess();
	}

	public static interface PeriodReadValue extends NullableReadValue {
		Period getPeriod();
	}

	public static interface PeriodWriteValue extends NullableWriteValue {
		void setPeriod(Period period);
	}

	static class PeriodAccess extends AbstractColumnDataAccess<PeriodData>
			implements PeriodReadValue, PeriodWriteValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setPeriod(Period Period) {
			m_data.setPeriod(m_index, Period);
		}

		@Override
		public Period getPeriod() {
			return m_data.getPeriod(m_index);
		}

	}
}