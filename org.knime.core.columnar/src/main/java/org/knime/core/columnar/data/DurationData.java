package org.knime.core.columnar.data;

import java.time.Duration;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface DurationData extends NullableColumnData {
	void setDuration(int index, Duration localDateTime);

	Duration getDuration(int index);

	class DurationDataSpec implements ColumnDataSpec<DurationData> {
		@Override
		public Class<? extends DurationData> getColumnDataType() {
			return DurationData.class;
		}
	}
}