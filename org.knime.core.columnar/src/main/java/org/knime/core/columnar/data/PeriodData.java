package org.knime.core.columnar.data;

import java.time.Period;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface PeriodData extends NullableColumnData {

	void setPeriod(int index, Period Period);

	Period getPeriod(int index);

	public final class PeriodDataSpec implements ColumnDataSpec<PeriodData> {
		@Override
		public Class<? extends PeriodData> getColumnDataType() {
			return PeriodData.class;
		}
	}
}