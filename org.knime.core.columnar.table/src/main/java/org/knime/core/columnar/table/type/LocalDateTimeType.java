package org.knime.core.columnar.table.type;

import java.time.LocalDateTime;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.LocalDateTimeData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class LocalDateTimeType implements ColumnType<LocalDateTimeData> {

	@Override
	public ColumnDataSpec<LocalDateTimeData> getChunkSpec() {
		return new LocalDateTimeData.LocalDateTimeDataSpec();
	}

	@Override
	public LocalDateTimeAccess createAccess() {
		return new LocalDateTimeAccess();
	}

	public static interface LocalDateTimeReadValue extends NullableReadValue {
		LocalDateTime getLocalDateTime();
	}

	public static interface LocalDateTimeWriteValue extends NullableWriteValue {
		void setLocalDateTime(LocalDateTime dateTime);
	}

	static class LocalDateTimeAccess extends AbstractColumnDataAccess<LocalDateTimeData>
			implements LocalDateTimeReadValue, LocalDateTimeWriteValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setLocalDateTime(LocalDateTime localDateTime) {
			m_data.setLocalDateTime(m_index, localDateTime);
		}

		@Override
		public LocalDateTime getLocalDateTime() {
			return m_data.getLocalDateTime(m_index);
		}

	}

}