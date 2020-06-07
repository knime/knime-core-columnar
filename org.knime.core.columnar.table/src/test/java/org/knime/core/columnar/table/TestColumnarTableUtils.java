package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStoreUtils;
import org.knime.core.columnar.TestColumnStoreFactory;
import org.knime.core.columnar.chunk.ColumnSelection;
import org.knime.core.columnar.data.DoubleData;

class TestColumnarTableUtils {

	static final WriteTable createWriteTable(int numColumns, int chunkCapacity) {
		final TableSchema schema = schema(numColumns);
		return ColumnarTableUtils.createWriteTable(schema,
				new TestColumnStoreFactory().createWriteStore(schema, null, chunkCapacity));
	}

	static final WriteTable createWriteTableWithCache(int numColumns, int chunkCapacity) {
		final TableSchema schema = schema(numColumns);
		return ColumnarTableUtils.createWriteTable(schema,
				ColumnStoreUtils.cache(new TestColumnStoreFactory().createWriteStore(schema, null, chunkCapacity)));
	}

	static final TableSchema schema(int numColumns) {
		return new TableSchema() {

			@Override
			public int getNumColumns() {
				return numColumns;
			}

			@Override
			public ColumnType<?> getColumnSpec(int idx) {
				return new TestDoubleColumnType();
			}
		};
	}

	static TableReadFilter createTableReadCursorConfig(long minRowIndex, long maxRowIndex,
			int... selectedColumns) {
		return new TableReadFilter() {

			@Override
			public ColumnSelection getColumnSelection() {
				return new ColumnSelection() {

					@Override
					public int[] get() {
						return selectedColumns;
					}
				};
			}

			@Override
			public long getMinRowIndex() {
				return minRowIndex;
			}

			@Override
			public long getMaxRowIndex() {
				return maxRowIndex;
			}

		};
	}

	static class TestDoubleColumnType implements ColumnType<DoubleData> {

		public interface TestDoubleReadValue extends NullableReadValue {
			double getDouble();
		}

		public interface TestDoubleWriteValue extends NullableWriteValue {
			void setDouble(double val);
		}

		@Override
		public ColumnDataSpec<DoubleData> getColumnDataSpec() {
			return new ColumnDataSpec<DoubleData>() {

				@Override
				public Class<? extends DoubleData> getColumnDataType() {
					return DoubleData.class;
				}
			};
		}

		@Override
		public ColumnDataAccess<DoubleData> createAccess() {
			return new TestDoubleDataAccess();
		}

		class TestDoubleDataAccess extends AbstractColumnDataAccess<DoubleData>
				implements TestDoubleReadValue, TestDoubleWriteValue {

			@Override
			public boolean isMissing() {
				return m_data.isMissing(m_index);
			}

			@Override
			public void setDouble(double val) {
				m_data.setDouble(m_index, val);
			}

			@Override
			public double getDouble() {
				return m_data.getDouble(m_index);
			}

			@Override
			public void setMissing() {
				m_data.setMissing(m_index);
			}
		}
	}

}
