package org.knime.core.columnar.data;

import java.time.LocalDateTime;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface LocalDateTimeData extends NullableColumnData {
	void setLocalDateTime(int index, LocalDateTime localDateTime);

	LocalDateTime getLocalDateTime(int index);

	public final class LocalDateTimeDataSpec implements ColumnDataSpec<LocalDateTimeData> {
		@Override
		public Class<? extends LocalDateTimeData> getColumnDataType() {
			return LocalDateTimeData.class;
		}
	}

}