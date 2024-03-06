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

    /*
     * TODO(heap-badger) Currently, we initialize m_numRows with -1 if we do not know the total number of rows. We only
     * set it to the real value after reading the last batch. This won't be necessary anymore with the heap-badger
     * changes because the BatchReadStore will have API to get the total number of rows without reading the last batch.
     *
     * See TODO(numRows)
     */

    private final Selection m_selection;

    private final ColumnarReadAccess[] m_accesses;

    private final RandomAccessBatchReader m_reader;

    private final int m_numBatches;

    private final int m_batchLength;

    private ReadBatch m_currentBatch;

    /** Row inside the selection */
    private long m_nextRow;

    /** Number of rows of the selection */
    private long m_numRows;

    private int m_indexInBatch;

    private long m_firstRowInStore;

    private int m_currentBatchIdx;

    public DefaultColumnarCursor(final BatchReadStore store, final Selection selection) {
        var schema = store.getSchema();
        m_selection = Objects.requireNonNull(selection);

        // Initialize accesses
        m_accesses = createReadAccesses(schema.specStream(), () -> m_indexInBatch);

        // Initialize reader
        m_numBatches = store.numBatches();
        m_batchLength = store.batchLength();
        m_reader = store.createRandomAccessReader(ColumnSelection.fromSelection(selection, schema.numColumns()));

        // Initialize the iteration indices
        m_nextRow = 0;
        m_firstRowInStore = Math.max(selection.rows().fromIndex(), 0);
        // TODO(numRows) set the real value of numRows and make final
        m_numRows = m_batchLength == 0 ? 0 : (selection.rows().toIndex() - m_firstRowInStore);

        m_currentBatchIdx = -1;
        m_indexInBatch = -1;
    }

    @Override
    public ReadAccessRow access() {
        return this;
    }

    // TODO abstract implementation of RandomAccessCursor for forward and canForward based on moveTo
    @Override
    public boolean canForward() {
        // TODO(numRows) remove the < 0 check
        return m_numRows < 0 || m_nextRow < m_numRows;
    }

    @Override
    public boolean forward() {
        if (canForward()) {
            moveTo(m_nextRow);
            m_nextRow++;
            return true;
        }
        return false;
    }

    private int getBatchIdx(final long storeRow) {
        // TODO(numRows) - adapt to variable batch sizes
        return (int)(storeRow / m_batchLength);
    }

    @Override
    public void moveTo(final long row) {
        // TODO(numRows) remove the m_numRows >= 0 check
        if (row < 0 || (m_numRows >= 0 && row >= m_numRows)) {
            throw new IndexOutOfBoundsException(row);
        }

        var storeRow = row + m_firstRowInStore;
        switchToBatch(getBatchIdx(storeRow));
        m_indexInBatch = (int)(storeRow % m_batchLength);
    }

    /** Switch to the given batch if it is not the current batch */
    private void switchToBatch(final int idx) {
        if (m_currentBatchIdx == idx) {
            // We are already there
            return;
        }

        // Release the old batch
        if (m_currentBatch != null) {
            m_currentBatch.release();
        }

        // Read the new batch
        m_currentBatchIdx = idx;
        try {
            m_currentBatch = m_reader.readRetained(m_currentBatchIdx);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        // HACK: Set the numRows if this is the last batch
        // TODO(numRows) remove the hack
        if (m_numRows < 0 && m_currentBatchIdx == m_numBatches - 1) {
            m_numRows = (m_numBatches - 1) * (long)m_batchLength + m_currentBatch.length();
        }

        // Update the accesses
        final NullableReadData[] currentData = m_currentBatch.getUnsafe();
        for (var i = 0; i < currentData.length; i++) {
            if (m_selection.columns().isSelected(i)) {
                m_accesses[i].setData(currentData[i]);
            }
        }
    }

    @Override
    public void close() throws IOException {
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
    public <A extends ReadAccess> A getAccess(final int index) {
        return (A)m_accesses[index];
    }
}
