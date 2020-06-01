package org.knime.core.columnar.table.type;

import java.time.LocalDate;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.LocalDateData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class LocalDateType implements ColumnType<LocalDateData> {

	@Override
	public ColumnDataSpec<LocalDateData> getColumnDataSpec() {
		return new LocalDateData.LocalDateDataSpec();
	}

	@Override
	public LocalDateAccess createAccess() {
		return new LocalDateAccess();
	}

	public static interface LocalDateReadValue extends NullableReadValue {
		LocalDate getLocalDate();
	}

	public static interface LocalDateWriteValue extends NullableWriteValue {
		void setLocalDate(LocalDate localDate);
	}

	static class LocalDateAccess extends AbstractColumnDataAccess<LocalDateData>
			implements LocalDateReadValue, LocalDateWriteValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setLocalDate(LocalDate LocalDate) {
			m_data.setLocalDate(m_index, LocalDate);
		}

		@Override
		public LocalDate getLocalDate() {
			return m_data.getLocalDate(m_index);
		}

	}
}