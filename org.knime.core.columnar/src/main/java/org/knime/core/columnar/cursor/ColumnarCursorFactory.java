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
 *   22 Apr 2021 (Marc): created
 */
package org.knime.core.columnar.cursor;

import org.knime.core.columnar.filter.BatchRange;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;
import org.knime.core.table.virtual.EmptyCursor;

/**
 * Creates {@link Cursor Cursors} to read from {@link BatchReadStore data storages}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ColumnarCursorFactory {

    private ColumnarCursorFactory() {
    }

    /**
     * Creates a {@link RandomAccessCursor} that reads from the provided {@link BatchReadStore}.
     *
     * @param readStore the store
     * @param size the number of rows in the readStore
     * @return {@link RandomAccessCursor} reading all entries in the store
     */
    public static RandomAccessCursor<ReadAccessRow> create(final BatchReadStore readStore, final long size) {
        if (size == 0) {
            return new EmptyCursor(readStore.getSchema());
        } else {
            return create(readStore, Selection.all().retainRows(0, size));
        }
    }

    /**
     * Creates a {@link RandomAccessCursor} that reads from the provided {@link BatchReadStore}.
     *
     * @param store to read from
     * @param selection the columns and row range to read
     * @return a {@link RandomAccessCursor} that reads from {@link BatchReadStore store}
     */
    public static RandomAccessCursor<ReadAccessRow> create(final BatchReadStore store, final Selection selection) {
        return new DefaultColumnarCursor(store, selection);
    }

    /**
     * Creates a {@link RandomAccessCursor} that reads from the provided {@link BatchReadStore}.
     *
     * @param store to read from
     * @param selection the columns to read
     * @param batchRange the range of batches and rows to read
     * @return a {@link RandomAccessCursor} that reads from {@link BatchReadStore store}
     */
    public static RandomAccessCursor<ReadAccessRow> create(final BatchReadStore store, final ColumnSelection selection,
        final BatchRange batchRange) {
        throw new IllegalStateException("BatchRange will be removed");
    }
}
