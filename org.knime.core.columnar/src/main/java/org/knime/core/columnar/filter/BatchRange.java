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
            final long firstRow = selection.rows().fromIndex();
            final long lastRow = selection.rows().toIndex() - 1;
            final int batchLen = store.batchLength();
            final int firstBatch = (int)(firstRow / batchLen);
            final int firstRowInFirstBatch = (int)(firstRow % batchLen);
            final int lastBatch = (int)(lastRow / batchLen);
            final int lastIndexInLastBatch = (int)(lastRow % batchLen);
            return new DefaultBatchRange(firstBatch, firstRowInFirstBatch, lastBatch, lastIndexInLastBatch);
        }
    }
}
