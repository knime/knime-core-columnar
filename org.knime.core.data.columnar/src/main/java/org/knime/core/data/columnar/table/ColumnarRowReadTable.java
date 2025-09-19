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
 *   Oct 28, 2021 (marcel): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;

import org.knime.core.columnar.cursor.ColumnarCursorFactory;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.columnar.filter.TableFilterUtils;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.table.DefaultColumnarBatchReadStore.ColumnarBatchReadStoreBuilder;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;

/**
 * Standard implementation of a read-only table that understands KNIME's {@link ReadValue logical} data types and is
 * backed by a columnar store. <br>
 * Contrary to its write-only {@link ColumnarRowWriteTable counterpart}, this table does <em>not</em> discard its
 * contained data when being closed. Clients intending to clear the data need to physically delete the contained
 * {@link #getStore() store} after closing the table.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarRowReadTable implements RandomRowAccessible {

    /** a system property to disable all caches in a read table for testing */
    private static final String DISABLE_CACHES_SYS_PROP = "knime.columnar.disablecaches.readtable";

    private final ValueSchema m_schema;

    private final ColumnStoreFactory m_storeFactory;

    private final ColumnarBatchReadStore m_store;

    private final long m_size;

    private final CursorTracker<RandomAccessCursor<ReadAccessRow>> m_cursorTracker =
        CursorTracker.createRandomAccessCursorTracker();

    private final CursorTracker<RowCursor> m_rowCursorTracker = CursorTracker.createRowCursorTracker();

    /**
     * Note: Used when loading a {@link ColumnarRowReadTable} from disk
     *
     * @param schema The schema of the table.
     * @param storeFactory The factory which created the table's underlying store.
     * @param store The table's underlying store.
     * @param size The number of rows contained in the table.
     */
    @SuppressWarnings("resource") // Wrapped store will be closed along with its wrapper, i.e. along with this table.
    public ColumnarRowReadTable(final ValueSchema schema, final ColumnStoreFactory storeFactory,
        final BatchReadStore store, final long size) {
        this(schema, storeFactory, wrapInColumnarStore(store), size);
    }

    private static ColumnarBatchReadStore wrapInColumnarStore(final BatchReadStore store) {
        var builder = new ColumnarBatchReadStoreBuilder(store) //
            .enableDictEncoding(true); //
        if (!Boolean.getBoolean(DISABLE_CACHES_SYS_PROP)) {
            builder.useColumnDataCache(ColumnarPreferenceUtils.getColumnDataCache()) //
                .useHeapCache(ColumnarPreferenceUtils.getHeapCache()); //

            // NOTE:
            // We do not use the ReadBatchCache for now because it can cause a deadlock on a memory alert.
            // The cache will be useful when we have random-access rows but this is not the case yet.
            // .useReadBatchCache(ColumnarPreferenceUtils.getReadBatchCache())
        }
        return builder.build();
    }

    /**
     * @param schema The schema of the table.
     * @param storeFactory The factory which created the table's underlying store.
     * @param store The table's underlying store.
     * @param size The number of rows contained in the table.
     */
    public ColumnarRowReadTable(final ValueSchema schema, final ColumnStoreFactory storeFactory,
        final ColumnarBatchReadStore store, final long size) {
        m_schema = schema;
        m_storeFactory = storeFactory;
        m_store = store;
        m_size = size;
    }

    /**
     * @return This table's schema.
     */
    @Override
    public ValueSchema getSchema() {
        return m_schema;
    }

    /**
     * @return The factory which created this table's underlying store.
     */
    public ColumnStoreFactory getStoreFactory() {
        return m_storeFactory;
    }

    /**
     * @return This table's underlying store.
     */
    public ColumnarBatchReadStore getStore() {
        return m_store;
    }

    /**
     * @return The number of rows contained in this table.
     */
    @Override
    public long size() {
        return m_size;
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor() {
        // we track the cursors, so that we can close them before closing m_store
        return m_cursorTracker.createTrackedCursor(() -> ColumnarCursorFactory.create(m_store, m_size));
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor(final Selection selection) {
        // we track the cursors, so that we can close them before closing m_store
        return m_cursorTracker.createTrackedCursor(() -> ColumnarCursorFactory.create(m_store, selection));
    }

    /**
     * @return A newly constructed cursor that allows to read this table's data.
     */
    // TODO (TP): This method probably shouldn't be here but in AbstractColumnarContainerTable!
    //            (which is the only place it's called).
    //            I guess it is here because of the Cursor tracking?
    //            Maybe that is even unnecessary because we could (and do) track the underlying RandomAccessCursor<ReadAccessRow>?
    //            Either that, or the m_rowCursorTracker should move to AbstractColumnarContainerTable as well.
    public RowCursor createRowCursor() {
        return m_rowCursorTracker.createTrackedCursor(() -> ColumnarRowCursorFactory.create(m_store, m_schema, m_size));
    }

    /**
     * @param filter A filter that constrains which rows and columns of this table's data will be accessed by the
     *            cursor.
     * @return A newly constructed cursor that allows to read this table's data.
     */
    // TODO (TP): This method probably shouldn't be here but in AbstractColumnarContainerTable!
    //            (which is the only place it's called).
    //            I guess it is here because of the Cursor tracking?
    //            Maybe that is even unnecessary because we could (and do) track the underlying RandomAccessCursor<ReadAccessRow>?
    //            Either that, or the m_rowCursorTracker should move to AbstractColumnarContainerTable as well.
    public RowCursor createRowCursor(final TableFilter filter) {
        if (filter != null) {
            validateSelection(TableFilterUtils.toSelection(filter, m_size), m_schema.numColumns(), m_size);
        }
        return m_rowCursorTracker
            .createTrackedCursor(() -> ColumnarRowCursorFactory.create(m_store, m_schema, m_size, filter));
    }

    /**
     * Validates a {@code Selection} against table extents.
     *
     * @param selection the selection to validate
     * @param numCols number of columns of the table that is to be filtered
     * @param numRows number of rows of the table that is to be filtered
     * @throws IndexOutOfBoundsException when any index is out of bounds
     */
    // TODO(AP-25045) Move to Selection interface as default method
    private static void validateSelection(final Selection selection, final int numCols, final long numRows) {
        if (!selection.columns().allSelected()) {
            final int[] cols = selection.columns().getSelected();
            if (cols.length > 0) {
                if (cols[0] < 0) {
                    throw new IndexOutOfBoundsException("Selecting column at index " + cols[0]);
                }
                if (cols[cols.length - 1] >= numCols) {
                    throw new IndexOutOfBoundsException("Selecting column at index " + cols[cols.length - 1]
                        + " in a table with " + numCols + " columns");
                }
            }
        }

        if (!selection.rows().allSelected() && selection.rows().toIndex() > numRows) {
            throw new IndexOutOfBoundsException(
                "Selecting to row index " + selection.rows().toIndex() + " in a table with " + numRows + " rows");
        }
    }

    @Override
    public void close() throws IOException {
        m_rowCursorTracker.close();
        m_cursorTracker.close();
        m_store.close();
    }

}
