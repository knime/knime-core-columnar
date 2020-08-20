/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStoreUtils;
import org.knime.core.columnar.TestColumnStoreFactory;
import org.knime.core.columnar.chunk.ColumnSelection;
import org.knime.core.columnar.data.DoubleData;

class ColumnarTableTestUtils {

    static final WriteTable createWriteTable(final int numColumns, final int chunkCapacity) {
        final TableSchema schema = schema(numColumns);
        return ColumnarTableUtils.createWriteTable(schema,
            new TestColumnStoreFactory().createWriteStore(schema, null, chunkCapacity));
    }

    static final WriteTable createWriteTableWithCache(final int numColumns, final int chunkCapacity) {
        final TableSchema schema = schema(numColumns);
        return ColumnarTableUtils.createWriteTable(schema,
            ColumnStoreUtils.cache(new TestColumnStoreFactory().createWriteStore(schema, null, chunkCapacity)));
    }

    static final TableSchema schema(final int numColumns) {
        return new TableSchema() {

            @Override
            public int getNumColumns() {
                return numColumns;
            }

            @Override
            public ColumnType<?> getColumnSpec(final int idx) {
                return new TestDoubleColumnType();
            }
        };
    }

    static TableReadFilter createTableReadCursorConfig(final long minRowIndex, final long maxRowIndex, final int... selectedColumns) {
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
            public void setDouble(final double val) {
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
