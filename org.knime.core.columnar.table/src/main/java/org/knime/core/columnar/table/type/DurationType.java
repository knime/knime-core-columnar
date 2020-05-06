package org.knime.core.columnar.table.type;

import java.time.Duration;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.DurationData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class DurationType implements ColumnType<DurationData> {

	@Override
	public DurationAccess createAccess() {
		return new DurationAccess();
	}

	@Override
	public ColumnDataSpec<DurationData> getChunkSpec() {
		return new DurationData.DurationDataSpec();
	}

	public static interface DurationReadValue extends NullableReadValue {
		Duration getDuration();
	}

	public static interface DurationWriteValue extends NullableWriteValue {
		void setDuration(Duration duration);
	}

	static class DurationAccess extends AbstractColumnDataAccess<DurationData>
			implements DurationReadValue, DurationWriteValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setDuration(Duration duration) {
			m_data.setDuration(m_index, duration);
		}

		@Override
		public Duration getDuration() {
			return m_data.getDuration(m_index);
		}
	}

}