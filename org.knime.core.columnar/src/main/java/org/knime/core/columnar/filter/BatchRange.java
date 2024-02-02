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
 *   Jul 22, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.filter;

import java.io.IOException;

import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.table.row.Selection;

/**
 * Represents the selection of a range of batches.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface BatchRange {

    /**
     * @return index of the first batch to include
     */
    int getFirstBatch();

    /**
     * @return index of the first row to include from the first batch
     */
    int getFirstRowInFirstBatch();

    /**
     * @return index of the last batch to include
     */
    int getLastBatch();

    /**
     * @return index of the last row to include from the last batch. Can be {@code -1} in which case the last batch is
     *         read fully.
     */
    int getLastRowInLastBatch();

    /**
     * Create a new {@link BatchRange} from the row range in the given {@link Selection}.
     *
     * @param selection selected columns and row range
     * @param store the store provides the batch length and number of batches
     * @return a new {@link BatchRange}
     */
    static BatchRange fromSelection(final Selection selection, final BatchReadStore store) {
        if (selection.rows().allSelected()) {
            final int lastIndexInLastBatch = -1; // last batch should be read fully
            return new DefaultBatchRange(0, 0, store.numBatches() - 1, lastIndexInLastBatch);
        } else {
            try {
                var batchBoundaries = findBatchBoundaries(store);
                int lastBatchIndex = batchBoundaries.length - 1;

                if (lastBatchIndex < 0) {
                    throw new IllegalStateException("No batches present");
                }

                final long fromRowIndex = Math.max(selection.rows().fromIndex(), 0);
                final long toRowIndex = selection.rows().toIndex();

                if (toRowIndex >= batchBoundaries[batchBoundaries.length - 1]) {
                    throw new IllegalStateException("Selection extends past the end of the ReadStore (selected to "
                        + toRowIndex + ", but only " + batchBoundaries[batchBoundaries.length - 1] + " rows available).");
                }

                int firstBatchIndex = 0;
                while (fromRowIndex >= batchBoundaries[firstBatchIndex]) {
                    firstBatchIndex++;
                }
                int firstIndexInFirstBatch =
                    (int)(fromRowIndex - (firstBatchIndex == 0 ? 0 : batchBoundaries[firstBatchIndex - 1]));

                while (lastBatchIndex > 0 && toRowIndex < batchBoundaries[lastBatchIndex - 1]) {
                    lastBatchIndex--;
                }
                int lastIndexInLastBatch =
                    (int)(toRowIndex - (lastBatchIndex == 0 ? 0 : batchBoundaries[lastBatchIndex - 1]));

                return new DefaultBatchRange(firstBatchIndex, firstIndexInFirstBatch, lastBatchIndex,
                    lastIndexInLastBatch);
            } catch (IOException ex) {
                throw new IllegalStateException("Couldn't find batch boundaries in store.", ex);
            }
        }
    }

    /**
     * Find the boundaries of (variably sized) batches in the store.
     *
     * NOTE: not optimal as it loads each batch!
     *
     * @param store
     * @return an array of offsets for the start of the next batch, so the first value = num rows of the first batch,
     *         the second value indicates the end of the second batch etc
     * @throws IOException
     */
    private static long[] findBatchBoundaries(final BatchReadStore store) throws IOException {
        // TODO: don't read all batches for this, read batch boundaries from footer
        int numBatches = store.numBatches();
        long[] batchBoundaries = new long[numBatches];
        try (var batchReadable = store.createRandomAccessReader()) {
            for (int i = 0; i < numBatches; i++) {
                var batch = batchReadable.readRetained(i);
                batchBoundaries[i] = batch.length() + (i == 0 ? 0 : batchBoundaries[i - 1]);
                batch.release();
            }
            return batchBoundaries;
        }
    }
}
