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
 *
 * History
 *   Mar 17, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cursor;

import java.io.IOException;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cursor.ColumnarReadAccessRowFactory.ColumnarReadAccessRow;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Provides random access via row index on a BatchReadStore.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
//TODO (TP) rename to ColumnarRowAccessible
//TODO (TP) move to more appropriate package (with ColumnarReadAccessRowFactory and ColumnarReadAccessRowFactoryTest)
public final class ColumnarRandomRowAccessible implements RandomRowAccessible {

    private final RandomAccessBatchReadable m_store;

    private final long m_numRows;

    private final int m_batchLength;

    private final ColumnarReadAccessRowFactory m_rowFactory;

    public ColumnarRandomRowAccessible(final RandomAccessBatchReadable store, final long numRows, final int batchLength) {
        m_store = store;
        m_numRows = numRows;
        m_batchLength = batchLength;
        m_rowFactory = new ColumnarReadAccessRowFactory(getSchema());
    }

    @Override
    public long size() {
        return m_numRows;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_store.getSchema();
    }

    @Override
    public void close() throws IOException {
        m_store.close();
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor() {
        return createCursor(Selection.all());
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor(final Selection selection) {
        final var indexInBatch = new MutableColumnDataIndex();
        var row = m_rowFactory.createRow(indexInBatch, selection.columns());
        return new ColumnarRandomAccessCursor(row, indexInBatch, selection);
    }

    /**
     * ColumnDataIndex that can be modified via the setIndex method.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private static final class MutableColumnDataIndex implements ColumnDataIndex {

        private int m_index;

        @Override
        public int getIndex() {
            return m_index;
        }

        void setIndex(final int index) {
            m_index = index;
        }

        public void increment() {
            ++m_index;
        }
    }

    private final class ColumnarRandomAccessCursor implements RandomAccessCursor<ReadAccessRow> {

        private final RandomAccessBatchReader m_batchReader;

        private final MutableColumnDataIndex m_indexInBatch;

        private final ColumnarReadAccessRow m_readAccessRow;

        // first row in selection (inclusive)
        private final long m_from;

        // last row + 1 in selection (exclusive)
        private final long m_to;

        // first row in the current batch (inclusive)
        private long m_batchFromRow = Long.MAX_VALUE;

        // last row + 1 in the current batch (exclusive)
        private long m_batchToRow= -1;

        // the current row. relative to the start of the table
        private long m_row;

        private ReadBatch m_currentBatch;

        ColumnarRandomAccessCursor(final ColumnarReadAccessRow readAccessRow,
            final MutableColumnDataIndex indexInBatch, final Selection selection) {
            m_batchReader = m_store.createRandomAccessReader(convertColumnSelection(selection)); // TODO (TP): move convertColumnSelection() logic into m_store.createRandomAccessReader(...)
            m_readAccessRow = readAccessRow;
            m_indexInBatch = indexInBatch;
            var rowSelection = selection.rows();
            if (rowSelection.allSelected()) {
                m_from = 0;
                m_to = m_numRows;
            } else {
                m_from = rowSelection.fromIndex();
                m_to = rowSelection.toIndex();
            }
            m_row = m_from - 1;
        }

        private ColumnSelection convertColumnSelection(final Selection selection) {
            return ColumnSelection.fromSelection(selection, getSchema().numColumns());
        }

        @Override
        public boolean forward() {
            if (++m_row < m_batchToRow) {
                m_indexInBatch.increment();
            } else {
                // moving to a different batch
                // bounds checking first
                if (m_row >= m_to) {
                    return false;
                }
                switchCurrentBatch();
            }
            return true;
        }

        @Override
        public void moveTo(final long row) {
            // N.B. The row argument is relative to m_from.
            //      m_row is the "true" row-index.
            m_row = row + m_from;
            if (m_row >= m_batchFromRow && m_row < m_batchToRow) {
                m_indexInBatch.setIndex((int)(m_row - m_batchFromRow));
            } else {
                // moving to a different batch
                // bounds checking first
                if (m_row < m_from || m_row >= m_to) {
                    m_row = m_to; // so that canForward()/forward() will work correctly
                    throw new IndexOutOfBoundsException();
                }
                switchCurrentBatch();
            }
        }

        private void switchCurrentBatch() {
            final long batchIndex = m_row / m_batchLength;
            final long firstRowInBatch = m_batchLength * batchIndex;
            m_batchFromRow = Math.max(m_from, firstRowInBatch);
            m_batchToRow = Math.min(m_to, m_batchLength * (batchIndex + 1));

            releaseCurrentBatch();
            m_currentBatch = readBatch((int)batchIndex);
            m_readAccessRow.setBatch(m_currentBatch);

            m_indexInBatch.setIndex((int)(m_row - firstRowInBatch));
        }

        private void releaseCurrentBatch() {
            if (m_currentBatch != null) {
                m_currentBatch.release();
            }
        }

        private ReadBatch readBatch(final int batchIndex) {
            try {
                return m_batchReader.readRetained(batchIndex);
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to read batch at index " + batchIndex, ex);
            }
        }

        @Override
        public boolean canForward() {
            return (m_row + 1) < m_to;
        }

        @Override
        public ReadAccessRow access() {
            return m_readAccessRow;
        }

        @Override
        public void close() throws IOException {
            releaseCurrentBatch();
            m_batchReader.close();
        }

    }

}