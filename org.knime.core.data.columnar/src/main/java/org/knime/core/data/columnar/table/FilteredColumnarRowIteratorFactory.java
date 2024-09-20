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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.UnmaterializedCell;
import org.knime.core.data.columnar.table.ColumnarRowIterator.CellIterator;
import org.knime.core.data.columnar.table.ColumnarRowIterator.FilteredColumnarDataRow;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.table.row.Selection.ColumnSelection;

/**
 * Factory for {@link CloseableRowIterator CloseableRowIterators} supporting column selection.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class FilteredColumnarRowIteratorFactory {

    private abstract static class FilteredColumnarRowIterator extends CloseableRowIterator {

        final RowCursor m_cursor;

        FilteredColumnarRowIterator(final RowCursor cursor) {
            m_cursor = cursor;
        }

        @Override
        public final boolean hasNext() {
            return m_cursor.canForward();
        }

        @Override
        public final DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return nextInternal();
        }

        @Override
        public final void close() {
            m_cursor.close();
        }

        abstract DataRow nextInternal();

    }

    private static final class SingleCellRowIterator extends FilteredColumnarRowIterator {

        private final int m_colIdx;

        SingleCellRowIterator(final RowCursor cursor, final int colIdx) {
            super(cursor);
            m_colIdx = colIdx;
        }

        @Override
        final DataRow nextInternal() {
            final RowRead access = m_cursor.forward();
            final DataCell cell =
                access.isMissing(m_colIdx) ? MISSING_CELL : access.<ReadValue> getValue(m_colIdx).getDataCell();

            return new SingleCellDataRow(access.getRowKey().getString(), cell, m_colIdx, m_cursor.getNumColumns());
        }

    }

    private static final class SingleCellDataRow extends FilteredColumnarDataRow {

        private final DataCell m_cell;

        private final int m_index;

        public SingleCellDataRow(final String rowKey, final DataCell cell, final int index, final int numCells) {
            super(rowKey, numCells);
            m_index = index;
            m_cell = cell;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return new CellIterator() {
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
            };
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

    private static final class ArrayRowIterator extends FilteredColumnarRowIterator {

        private final int[] m_selection;

        ArrayRowIterator(final RowCursor cursor, final int[] selection) {
            super(cursor);
            m_selection = selection;
        }

        @Override
        public DataRow nextInternal() {
            final RowRead access = m_cursor.forward();
            final DataCell[] cells = new DataCell[m_cursor.getNumColumns()];
            for (int idx : m_selection) {
                cells[idx] = access.isMissing(idx) ? MISSING_CELL : access.<ReadValue> getValue(idx).getDataCell();
            }

            return new ArrayDataRow(access.getRowKey().getString(), cells, m_selection);
        }

    }

    private static final class ArrayDataRow extends FilteredColumnarDataRow {

        private final DataCell[] m_cells;

        private final int[] m_selection;

        ArrayDataRow(final String rowKey, final DataCell[] values, final int[] selection) {
            super(rowKey, values.length);
            m_cells = values;
            m_selection = selection;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return new CellIterator() {
                private int m_idx = 0;

                @Override
                public boolean hasNext() {
                    return m_idx < m_selection.length;
                }

                @Override
                public DataCell next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    final DataCell cell = getCell(m_selection[m_idx]);
                    m_idx++;
                    return cell;
                }
            };
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

    private static final class HashMapRowIterator extends FilteredColumnarRowIterator {

        private final int[] m_selection;

        HashMapRowIterator(final RowCursor cursor, final int[] selection) {
            super(cursor);
            m_selection = selection;
        }

        @Override
        public DataRow nextInternal() {
            final RowRead access = m_cursor.forward();
            final Map<Integer, DataCell> cells = new LinkedHashMap<>(m_selection.length);
            for (int selection : m_selection) {
                cells.put(selection,
                    access.isMissing(selection) ? MISSING_CELL : access.<ReadValue> getValue(selection).getDataCell());
            }
            return new HashMapDataRow(access.getRowKey().getString(), cells, m_cursor.getNumColumns());
        }

    }

    private static final class HashMapDataRow extends FilteredColumnarDataRow {

        private final Map<Integer, DataCell> m_cells;

        public HashMapDataRow(final String rowKey, final Map<Integer, DataCell> values, final int numCells) {
            super(rowKey, numCells);
            m_cells = values;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return new CellIterator() {
                private Iterator<DataCell> m_delegate = m_cells.values().iterator();

                @Override
                public boolean hasNext() {
                    return m_delegate.hasNext();
                }

                @Override
                public DataCell next() {
                    return m_delegate.next();
                }
            };
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
        return create(cursor, selection);
    }

    static CloseableRowIterator create(final RowCursor cursor, final ColumnSelection columnSelection) {
        // get the selected columns, not counting the row key at column 0, which is assumed to be always selected.
        final int[] cols = columnSelection.getSelected(1, cursor.getNumColumns() + 1);
        // adjust column indices for row key
        Arrays.setAll(cols, i -> cols[i] - 1);
        return create(cursor, cols);
    }

    private static CloseableRowIterator create(final RowCursor cursor, final int[] selection) {
        if (selection.length == 1) {
            return new SingleCellRowIterator(cursor, selection[0]);
        } else if (cursor.getNumColumns() <= WIDE_TABLE_THRESHOLD
            || ((double)selection.length / cursor.getNumColumns()) > SELECTION_DENSITY_THRESHOLD) {
            return new ArrayRowIterator(cursor, selection);
        } else {
            return new HashMapRowIterator(cursor, selection);
        }
    }

    private FilteredColumnarRowIteratorFactory() {
    }

}
