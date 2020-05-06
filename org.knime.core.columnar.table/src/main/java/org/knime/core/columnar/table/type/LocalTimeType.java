package org.knime.core.columnar.table.type;

import java.time.LocalTime;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.LocalTimeData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class LocalTimeType implements ColumnType<LocalTimeData> {

	@Override
	public ColumnDataSpec<LocalTimeData> getChunkSpec() {
		return new LocalTimeData.LocalTimeDataSpec();
	}

	@Override
	public LocalTimeAccess createAccess() {
		return new LocalTimeAccess();
	}

	public static interface LocalTimeReadValue extends NullableReadValue {
		LocalTime getLocalTime();
	}

	public static interface LocalTimeWriteValue extends NullableWriteValue {
		void setLocalTime(LocalTime localTime);
	}

	static class LocalTimeAccess extends AbstractColumnDataAccess<LocalTimeData>
			implements LocalTimeReadValue, LocalTimeWriteValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setLocalTime(LocalTime LocalTime) {
			m_data.setLocalTime(m_index, LocalTime);
		}

		@Override
		public LocalTime getLocalTime() {
			return m_data.getLocalTime(m_index);
		}

	}

}