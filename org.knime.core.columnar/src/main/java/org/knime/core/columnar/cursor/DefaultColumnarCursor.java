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
 *   Mar 1, 2024 (benjamin): created
 */
package org.knime.core.columnar.cursor;

import static org.knime.core.columnar.access.ColumnarAccessFactoryMapper.createReadAccesses;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;

import org.knime.core.columnar.access.ColumnarReadAccess;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;

/**
 * Default implementation of a {@link RandomAccessCursor} on a {@link BatchReadStore}. Supports selecting a range of
 * columns that are read from the store and a range of rows that the cursor operates on.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class DefaultColumnarCursor implements RandomAccessCursor<ReadAccessRow>, ReadAccessRow {

    private final Selection m_selection;

    private final ColumnarReadAccess[] m_accesses;

    private final RandomAccessBatchReader m_reader;

    private final long[] m_batchBoundaries;

    // index bounds for binary search on m_batchBoundaries.
    // (no need to look at batches outside the selection).
    private final int m_searchFrom;
    private final int m_searchTo;

    /**
     * The start (inclusive) row of the selection.
     * Row {@code i} returned by this cursor, is row {@code (i+m_fromRow)} in the BatchReadStore.
     */
    private final long m_fromRow;

    /**
     * Number of rows of the selection
     */
    private final long m_numRows;

    /**
     * Current row relative to the selection.
     */
    private long m_currentRow;

    /**
     * The batch containing the current row.
     */
    private ReadBatch m_currentBatch;

    /**
     * Index of the current row in the current batch.
     */
    private int m_indexInBatch;

    /**
     * The start (inclusive) row of the current batch, relative to the selection.
     */
    private long m_currentBatchFrom;

    /**
     * The end (exclusive) row of the current batch, relative to the selection.
     */
    private long m_currentBatchTo;

    /**
     * The offset between a row index of this cursor (i.e., relative to the selection)
     * and a row index in the current batch.
     */
    private long m_currentBatchOffset;

    public DefaultColumnarCursor(final BatchReadStore store, final Selection selection) {
        var schema = store.getSchema();
        m_selection = Objects.requireNonNull(selection);

        // Initialize accesses
        m_accesses = createReadAccesses(schema.specStream(), () -> m_indexInBatch);

        // Initialize reader
        m_batchBoundaries = store.getBatchBoundaries();
        m_reader = store.createRandomAccessReader(ColumnSelection.fromSelection(selection, schema.numColumns()));

        /**
         * The end (inclusive) row of the selection.
         * Row {@code i} returned by this cursor, is row {@code (i+m_fromRow)} in the BatchReadStore.
         */
        long m_toRow;
        if (selection.rows().allSelected()) {
            m_fromRow = 0;
            m_toRow = store.numRows();
        } else {
            m_fromRow = selection.rows().fromIndex();
            m_toRow = selection.rows().toIndex();
        }
        m_numRows = m_toRow - m_fromRow;

        m_searchFrom = findRange(m_batchBoundaries, m_fromRow, 0, m_batchBoundaries.length);
        m_searchTo = Math.min(m_batchBoundaries.length,
            findRange(m_batchBoundaries, m_toRow - 1, m_searchFrom, m_batchBoundaries.length) + 1);

        m_currentRow = -1;
        m_indexInBatch = -1;
    }

    @Override
    public ReadAccessRow access() {
        return this;
    }

    @Override
    public synchronized boolean canForward() {
        return m_currentRow < m_numRows - 1;
    }

    @Override
    public synchronized boolean forward() {
        if (canForward()) {
            moveTo(++m_currentRow);
            return true;
        }
        return false;
    }

    @Override
    public synchronized void moveTo(final long row) {
        // check whether row is still in current batch
        if (row < m_currentBatchFrom || row >= m_currentBatchTo) {
            // if not: check whether row is out-of-bounds
            if (row < 0 || row >= m_numRows) {
                throw new IndexOutOfBoundsException(row);
            }

            // switch to new batch
            final long row_store = row + m_fromRow;
            final int batchIndex = findRange(m_batchBoundaries, row_store, m_searchFrom, m_searchTo);
            switchToBatch(batchIndex);
        }
        m_currentRow = row;
        m_indexInBatch = (int)(row + m_currentBatchOffset);
    }

    /**
     * Switch to the given batch
     */
    private void switchToBatch(final int idx) {
        // Release the old batch
        if (m_currentBatch != null) {
            m_currentBatch.release();
        }

        // start (inclusive) and end (exclusive) row of the current batch, relative to the BatchReadStore.
        final long batchFrom_store = idx == 0 ? 0 : m_batchBoundaries[idx - 1];
        final long batchTo_store = m_batchBoundaries[idx];

        // start (inclusive) and end (exclusive) row of the current batch, relative to the selection.
        m_currentBatchFrom = Math.max(batchFrom_store - m_fromRow, 0);
        m_currentBatchTo = Math.min(batchTo_store - m_fromRow, m_numRows);

        // offset between a row index (relative to the selection), and a row index in the current batch.
        m_currentBatchOffset = m_fromRow - batchFrom_store;

        // Read the new batch
        try {
            m_currentBatch = m_reader.readRetained(idx);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        // Update the accesses
        final NullableReadData[] currentData = m_currentBatch.getUnsafe();
        for (var i = 0; i < currentData.length; i++) {
            if (m_selection.columns().isSelected(i)) {
                m_accesses[i].setData(currentData[i]);
            }
        }
    }

    /**
     * Returns the largest index {@code i} such that {@code values[i] < value}.
     */
    private static int findRange(final long[] values, final long value, final int fromIndex, final int toIndex) {
        final int i = Arrays.binarySearch(values, fromIndex, toIndex, value);
        return i < 0 ? -(i + 1) : (i + 1);
    }

    @Override
    public synchronized void close() throws IOException {
        if (m_currentBatch != null) {
            m_currentBatch.release();
            m_currentBatch = null;
        }
        m_reader.close();
    }

    // ReadAccessRow

    @Override
    public int size() {
        // number of columns
        return m_accesses.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <A extends ReadAccess> A getAccess(final int index) {
        return (A)m_accesses[index];
    }
}
