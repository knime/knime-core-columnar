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
import java.util.NoSuchElementException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;

/**
 * Implementation of a {@link CloseableRowIterator} via delegation to a {@link RowCursor}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarRowIterator extends CloseableRowIterator {

    private static final class ColumnStoreTableDataRow implements DataRow {

        private final String m_rowKey;

        private final DataCell[] m_cells;

        ColumnStoreTableDataRow(final String rowKey, final DataCell[] cells) {
            m_rowKey = rowKey;
            m_cells = cells;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return new Iterator<DataCell>() { // NOSONAR
                private int m_idx = 0;

                @Override
                public boolean hasNext() {
                    return m_idx < m_cells.length;
                }

                @Override
                public DataCell next() {
                    try {
                        final DataCell cell = getCell(m_idx);
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
            return m_cells[index];
        }
    }

    private static final DataCell MISSING_CELL = DataType.getMissingCell();

    private final RowCursor m_cursor;

    private final int m_numValues;

    ColumnarRowIterator(final RowCursor cursor) {
        m_cursor = cursor;
        m_numValues = m_cursor.getNumColumns();
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

        final DataCell[] cells = new DataCell[m_numValues];
        final RowRead access = m_cursor.forward();
        for (int i = 0; i < m_numValues; i++) {
            cells[i] = access.isMissing(i) ? MISSING_CELL : access.<ReadValue> getValue(i).getDataCell();
        }

        return new ColumnStoreTableDataRow(access.getRowKey().getString(), cells);
    }

    @Override
    public void close() {
        m_cursor.close();
    }

}
