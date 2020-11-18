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
package org.knime.core.data.columnar.table;

import java.util.HashMap;
import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.UnmaterializedCell;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;

/**
 * Factory for {@link CloseableRowIterator}s supporting column selection.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
class FilteredColumnarRowIterator {
    private FilteredColumnarRowIterator() {
    }

    /*
     *  Magic thresholds.
     *
     *  Idea: for small or dense selections we prefer DataCells.
     *  Only for large and sparse we want to use HashMap implementations.
     */
    private final static int WIDE_TABLE_THRESHOLD = 50;

    private final static double SELECTION_DENSITY_THRESHOLD = 0.7d;

    // TODO special case selection ranges, i.e. from column index x to y with x < y.
    static CloseableRowIterator create(final RowCursor cursor, final int[] selection) {
        if (selection.length == 1) {
            return new SingleCellRowIterator(cursor, selection[0]);
        } else if (selection.length <= WIDE_TABLE_THRESHOLD
            || ((double)selection.length / cursor.getNumColumns()) > SELECTION_DENSITY_THRESHOLD) {
            return new ArrayRowIterator(cursor, selection);
        } else {
            return new HashMapRowIterator(cursor, selection);
        }
    }

    private static final class HashMapRowIterator extends CloseableRowIterator {

        private final RowCursor m_cursor;

        private final int[] m_selection;

        HashMapRowIterator(final RowCursor cursor, final int[] selection) {
            m_cursor = cursor;
            m_selection = selection;
        }

        @Override
        public boolean hasNext() {
            return m_cursor.canForward();
        }

        @Override
        public DataRow next() {
            final RowRead access = m_cursor.forward();

            final HashMap<Integer, DataCell> cells = new HashMap<>(m_selection.length, 1.0f);
            for (int i = 0; i < m_selection.length; i++) {
                if (access.isMissing(m_selection[i])) {
                    cells.put(m_selection[i], DataType.getMissingCell());
                } else {
                    cells.put(m_selection[i], access.<ReadValue> getValue(m_selection[i]).getDataCell());
                }
            }
            return new HashMapDataRow(access.getRowKey().getString(), cells, m_cursor.getNumColumns());
        }

        @Override
        public void close() {
            m_cursor.close();
        }

        static class HashMapDataRow implements DataRow {

            private static final UnmaterializedCell INSTANCE = UnmaterializedCell.getInstance(); // TODO

            private final String m_rowKey;

            private final HashMap<Integer, DataCell> m_cells;

            private int m_numCells;

            public HashMapDataRow(final String rowKey, final HashMap<Integer, DataCell> values, final int numCells) {
                m_rowKey = rowKey;
                m_cells = values;
                m_numCells = numCells;
            }

            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {
                    int idx = 0;

                    @Override
                    public boolean hasNext() {
                        return idx < m_numCells;
                    }

                    @Override
                    public DataCell next() {
                        return getCell(idx++);
                    }
                };
            }

            @Override
            public int getNumCells() {
                return m_numCells;
            }

            @Override
            public RowKey getKey() {
                if (m_rowKey == null) {
                    /* TODO OK Behaviour? */
                    return null;
                }
                return new RowKey(m_rowKey);
            }

            @Override
            public DataCell getCell(final int index) {
                final DataCell dataCell = m_cells.get(index);
                if (dataCell == null) {
                    return INSTANCE;
                } else {
                    return dataCell;
                }
            }
        }
    }

    private static final class ArrayRowIterator extends CloseableRowIterator {

        private final RowCursor m_cursor;

        private final int[] m_selection;

        ArrayRowIterator(final RowCursor cursor, final int[] selection) {
            m_cursor = cursor;
            m_selection = selection;
        }

        @Override
        public DataRow next() {
            final RowRead access = m_cursor.forward();

            final DataCell[] cells = new DataCell[m_cursor.getNumColumns()];
            for (int i = 0; i < m_selection.length; i++) {
                if (access.isMissing(m_selection[i])) {
                    cells[m_selection[i]] = DataType.getMissingCell();
                } else {
                    // TODO performance!!
                    cells[m_selection[i]] = access.<ReadValue> getValue(m_selection[i]).getDataCell();
                }
            }
            return new ArrayDataRow(access.getRowKey().getString(), cells);
        }

        @Override
        public boolean hasNext() {
            return m_cursor.canForward();
        }

        @Override
        public void close() {
            m_cursor.close();
        }

        static class ArrayDataRow implements DataRow {

            private static final UnmaterializedCell INSTANCE = UnmaterializedCell.getInstance(); // TODO

            private final String m_rowKey;

            private final DataCell[] m_cells;

            public ArrayDataRow(final String rowKey, final DataCell[] values) {
                m_rowKey = rowKey;
                m_cells = values;
            }

            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {
                    int idx = 0;

                    @Override
                    public boolean hasNext() {
                        return idx < m_cells.length;
                    }

                    @Override
                    public DataCell next() {
                        return getCell(idx++);
                    }
                };
            }

            @Override
            public int getNumCells() {
                return m_cells.length;
            }

            @Override
            public RowKey getKey() {
                if (m_rowKey == null) {
                    /* TODO OK Behaviour? */
                    return null;
                }
                return new RowKey(m_rowKey);
            }

            @Override
            public DataCell getCell(final int index) {
                if (m_cells[index] == null) {
                    return INSTANCE;
                } else {
                    return m_cells[index];
                }
            }
        }
    }

    private final static class SingleCellRowIterator extends CloseableRowIterator {

        private final RowCursor m_cursor;

        private final int m_colIdx;

        SingleCellRowIterator(final RowCursor cursor, final int colIdx) {
            m_cursor = cursor;
            m_colIdx = colIdx;
        }

        @Override
        public boolean hasNext() {
            return m_cursor.canForward();
        }

        @Override
        public DataRow next() {
            final RowRead access = m_cursor.forward();

            final DataCell cell;
            if (access.isMissing(m_colIdx)) {
                cell = DataType.getMissingCell();
            } else {
                cell = access.<ReadValue> getValue(m_colIdx).getDataCell();
            }
            return new SingleCellDataRow(access.getRowKey().getString(), cell, m_colIdx, m_cursor.getNumColumns());
        }

        @Override
        public void close() {
            m_cursor.close();
        }

        static class SingleCellDataRow implements DataRow {

            private static final UnmaterializedCell INSTANCE = UnmaterializedCell.getInstance(); // TODO

            private final String m_rowKey;

            private final DataCell m_cell;

            private final int m_index;

            private final int m_numCells;

            public SingleCellDataRow(final String rowKey, final DataCell cell, final int index, final int numCells) {
                m_rowKey = rowKey;
                m_index = index;
                m_cell = cell;
                m_numCells = numCells;
            }

            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {
                    int idx = 0;

                    @Override
                    public boolean hasNext() {
                        return idx < 1;
                    }

                    @Override
                    public DataCell next() {
                        return getCell(idx++);
                    }
                };
            }

            @Override
            public int getNumCells() {
                return m_numCells;
            }

            @Override
            public RowKey getKey() {
                if (m_rowKey == null) {
                    /* TODO OK Behaviour? */
                    return null;
                }
                return new RowKey(m_rowKey);
            }

            @Override
            public DataCell getCell(final int index) {
                if (index == m_index) {
                    return m_cell;
                } else {
                    return INSTANCE;
                }
            }
        }
    }
}