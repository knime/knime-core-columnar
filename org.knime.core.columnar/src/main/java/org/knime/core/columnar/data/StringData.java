package org.knime.core.columnar.data;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.NullableColumnData;

public interface StringData extends NullableColumnData {
	String getString(int index);

	void setString(int index, String val);

	public final class StringDataSpec implements ColumnDataSpec<StringData> {

		private final boolean m_enableDict;

		public StringDataSpec(boolean enableDict) {
			m_enableDict = enableDict;
		}

		public boolean isDictEnabled() {
			return m_enableDict;
		}

		@Override
		public Class<? extends StringData> getColumnDataType() {
			return StringData.class;
		}
	}

}