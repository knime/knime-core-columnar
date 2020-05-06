package org.knime.core.columnar.data;

import java.time.LocalDate;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface LocalDateData extends NullableColumnData {
	void setLocalDate(int index, LocalDate LocalDate);

	LocalDate getLocalDate(int index);

	public class LocalDateDataSpec implements ColumnDataSpec<LocalDateData> {
		@Override
		public Class<? extends LocalDateData> getColumnDataType() {
			return LocalDateData.class;
		}
	}
}