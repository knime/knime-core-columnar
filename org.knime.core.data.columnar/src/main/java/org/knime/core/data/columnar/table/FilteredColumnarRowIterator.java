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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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
 * Factory for {@link CloseableRowIterator CloseableRowIterators} supporting column selection.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class FilteredColumnarRowIterator {

    private static final class SingleCellRowIterator extends CloseableRowIterator {

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
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final RowRead access = m_cursor.forward();
            final DataCell cell =
                access.isMissing(m_colIdx) ? MISSING_CELL : access.<ReadValue> getValue(m_colIdx).getDataCell();

            return new SingleCellDataRow(access.getRowKey().getString(), cell, m_colIdx, m_cursor.getNumColumns());
        }

        @Override
        public void close() {
            m_cursor.close();
        }

    }

    private static final class SingleCellDataRow implements DataRow {

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
                private boolean m_hasNext = true;

                @Override
                public boolean hasNext() {
                    return m_hasNext;
                }

                @Override
                public DataCell next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    m_hasNext = false;
                    return m_cell;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int getNumCells() {
            return m_numCells;
        }

        @Override
        public RowKey getKey() {
            return new RowKey(m_rowKey);
        }

        @Override
        public DataCell getCell(final int index) {
            if (index == m_index) {
                return m_cell;
            } else if (index < 0 || index >= m_numCells) {
                throw new IndexOutOfBoundsException();
            } else {
                return UNMATERIALIZED_CELL;
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
        public boolean hasNext() {
            return m_cursor.canForward();
        }

        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final RowRead access = m_cursor.forward();
            final DataCell[] cells = new DataCell[m_cursor.getNumColumns()];
            for (int idx : m_selection) {
                cells[idx] = access.isMissing(idx) ? MISSING_CELL : access.<ReadValue> getValue(idx).getDataCell();
            }

            return new ArrayDataRow(access.getRowKey().getString(), cells, m_selection);
        }

        @Override
        public void close() {
            m_cursor.close();
        }

    }

    private static final class ArrayDataRow implements DataRow {

        private final String m_rowKey;

        private final DataCell[] m_cells;

        private final int[] m_selection;

        ArrayDataRow(final String rowKey, final DataCell[] values, final int[] selection) {
            m_rowKey = rowKey;
            m_cells = values;
            m_selection = selection;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return new Iterator<DataCell>() { // NOSONAR
                private int m_idx = 0;

                @Override
                public boolean hasNext() {
                    return m_idx < m_selection.length;
                }

                @Override
                public DataCell next() {
                    try {
                        final DataCell cell = getCell(m_selection[m_idx]);
                        m_idx++;
                        return cell;
                    } catch (IndexOutOfBoundsException e) { // NOSONAR
                        throw new NoSuchElementException();
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int getNumCells() {
            return m_cells.length;
        }

        @Override
        public RowKey getKey() {
            return new RowKey(m_rowKey);
        }

        @Override
        public DataCell getCell(final int index) {
            if (m_cells[index] == null) {
                return UNMATERIALIZED_CELL;
            } else {
                return m_cells[index];
            }
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
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final RowRead access = m_cursor.forward();
            final Map<Integer, DataCell> cells = new LinkedHashMap<>(m_selection.length);
            for (int selection : m_selection) {
                cells.put(selection,
                    access.isMissing(selection) ? MISSING_CELL : access.<ReadValue> getValue(selection).getDataCell());
            }
            return new HashMapDataRow(access.getRowKey().getString(), cells, m_cursor.getNumColumns());
        }

        @Override
        public void close() {
            m_cursor.close();
        }

    }

    private static final class HashMapDataRow implements DataRow {

        private final String m_rowKey;

        private final Map<Integer, DataCell> m_cells;

        private int m_numCells;

        public HashMapDataRow(final String rowKey, final Map<Integer, DataCell> values, final int numCells) {
            m_rowKey = rowKey;
            m_cells = values;
            m_numCells = numCells;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return new Iterator<DataCell>() {
                private Iterator<DataCell> m_delegate = m_cells.values().iterator();

                @Override
                public boolean hasNext() {
                    return m_delegate.hasNext();
                }

                @Override
                public DataCell next() {
                    return m_delegate.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int getNumCells() {
            return m_numCells;
        }

        @Override
        public RowKey getKey() {
            return new RowKey(m_rowKey);
        }

        @Override
        public DataCell getCell(final int index) {
            final DataCell cell = m_cells.get(index);
            if (cell != null) {
                return cell;
            } else if (index < 0 || index >= m_numCells) {
                throw new IndexOutOfBoundsException();
            } else {
                return UNMATERIALIZED_CELL;
            }
        }

    }

    private static final DataCell MISSING_CELL = DataType.getMissingCell();

    private static final UnmaterializedCell UNMATERIALIZED_CELL = UnmaterializedCell.getInstance();

    /*
     *  Magic thresholds.
     *
     *  Idea: for small or dense selections we prefer DataCells.
     *  Only for large and sparse we want to use HashMap implementations.
     */
    private static final int WIDE_TABLE_THRESHOLD = 50;

    private static final double SELECTION_DENSITY_THRESHOLD = 0.7d;

    static CloseableRowIterator create(final RowCursor cursor, final Set<Integer> materializeColumnIndices) {
        final int[] selection = materializeColumnIndices.stream().sorted().mapToInt(Integer::intValue).toArray();
        if (selection.length == 1) {
            return new SingleCellRowIterator(cursor, selection[0]);
        } else if (cursor.getNumColumns() <= WIDE_TABLE_THRESHOLD
            || ((double)selection.length / cursor.getNumColumns()) > SELECTION_DENSITY_THRESHOLD) {
            return new ArrayRowIterator(cursor, selection);
        } else {
            return new HashMapRowIterator(cursor, selection);
        }
    }

    private FilteredColumnarRowIterator() {
    }

}
