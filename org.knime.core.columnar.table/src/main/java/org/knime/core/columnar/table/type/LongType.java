package org.knime.core.columnar.table.type;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.data.LongData;
import org.knime.core.columnar.table.AbstractColumnDataAccess;
import org.knime.core.columnar.table.ColumnType;
import org.knime.core.columnar.table.NullableReadValue;
import org.knime.core.columnar.table.NullableWriteValue;

public class LongType implements ColumnType<LongData> {

	@Override
	public ColumnDataSpec<LongData> getChunkSpec() {
		return new LongData.LongDataSpec();
	}

	@Override
	public LongAccess createAccess() {
		return new LongAccess();
	}

	public static interface LongReadValue extends NullableReadValue {
		long getLongValue();
	}

	public static interface LongWriteValue extends NullableWriteValue {
		void setLongValue(long value);
	}

	static class LongAccess extends AbstractColumnDataAccess<LongData> implements LongReadValue, LongWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setLongValue(long value) {
			m_data.setLong(m_index, value);
		}

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public long getLongValue() {
			return m_data.getLong(m_index);
		}
	}

}
