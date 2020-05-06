package org.knime.core.columnar.data;

import java.time.LocalTime;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface LocalTimeData extends NullableColumnData {

	void setLocalTime(int index, LocalTime LocalTime);

	LocalTime getLocalTime(int index);

	public final class LocalTimeDataSpec implements ColumnDataSpec<LocalTimeData> {
		@Override
		public Class<? extends LocalTimeData> getColumnDataType() {
			return LocalTimeData.class;
		}
	}
}