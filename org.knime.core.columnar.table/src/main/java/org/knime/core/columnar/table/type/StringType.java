package org.knime.core.columnar.table.type;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.StringData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class StringType implements ColumnType<StringData> {
	private final boolean m_dictEncoded;

	public StringType(boolean dictEncoded) {
		m_dictEncoded = dictEncoded;
	}

	@Override
	public ColumnDataSpec<StringData> getChunkSpec() {
		return new StringData.StringDataSpec(m_dictEncoded);
	}

	@Override
	public StringAccess createAccess() {
		return new StringAccess();
	}

	public static interface StringReadValue extends NullableReadValue {
		String getStringValue();
	}

	public static interface StringWriteValue extends NullableWriteValue {
		void setStringValue(String value);
	}

	public static class StringAccess extends AbstractColumnDataAccess<StringData>
			implements StringReadValue, StringWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setStringValue(String value) {
			m_data.setString(m_index, value);
		}

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public String getStringValue() {
			return m_data.getString(m_index);
		}
	}
}
