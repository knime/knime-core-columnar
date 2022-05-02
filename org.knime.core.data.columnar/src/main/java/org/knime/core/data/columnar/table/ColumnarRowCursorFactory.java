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

import java.io.IOException;

import org.knime.core.columnar.cursor.ColumnarCursorFactory;
import org.knime.core.columnar.filter.BatchRange;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.DefaultBatchRange;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.data.columnar.filter.TableFilterUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.virtual.VirtualTableUtils;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;

/**
 * Columnar implementations of {@link RowCursor} for reading data from columnar table backend.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ColumnarRowCursorFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarRowCursorFactory.class);

    private static final class EmptyRowCursor implements RowCursor {

        private final ColumnarValueSchema m_schema;

        private EmptyRowCursor(final ColumnarValueSchema schema) {
            m_schema = schema;
        }

        @Override
        public RowRead forward() {
            return null;
        }

        @Override
        public boolean canForward() {
            return false;
        }

        @Override
        public void close() {
            // this cursor holds no resources
        }

        @Override
        public int getNumColumns() {
            return m_schema.numColumns() - 1;
        }

    }

    private static final class DefaultRowCursor implements RowCursor {

        private final LookaheadCursor<ReadAccessRow> m_delegate;

        private final RowRead m_rowRead;

        private DefaultRowCursor(final LookaheadCursor<ReadAccessRow> delegate, final ColumnarValueSchema schema,
            final ColumnSelection selection) {
            m_delegate = delegate;
            ReadAccessRow access = delegate.access();
            m_rowRead = VirtualTableUtils.createRowRead(schema, access, selection);
        }

        @Override
        public boolean canForward() {
            return m_delegate.canForward();
        }

        @Override
        public RowRead forward() {
            return m_delegate.forward() ? m_rowRead : null;
        }

        @Override
        public int getNumColumns() {
            return m_rowRead.getNumColumns();
        }

        @Override
        public void close() {
            try {
                m_delegate.close();
            } catch (IOException ex) {
                final String error = "Exception while closing batch reader.";
                LOGGER.error(error, ex);
                throw new IllegalStateException(error, ex);
            }
        }

    }

    public static RowCursor create(final BatchReadStore store, final ColumnarValueSchema schema, final long size) {
        return create(store, schema, size, null);
    }

    @SuppressWarnings("resource") // the returned cursor has to be closed by the caller
    static RowCursor create(final BatchReadStore store, final ColumnarValueSchema schema, final long size, // NOSONAR
        final TableFilter filter) {
        if (size < 1) {
            return new EmptyRowCursor(schema);
        }

        // TODO: The following is very weird. Probably the exception message should be changed? And also the maxLength variable name?
        final int maxLength = store.batchLength();
        if (maxLength < 1) {
            throw new IllegalStateException(
                String.format("Length of table is %d, but maximum batch length is %d.", size, maxLength));
        }

        final var batchRange = createBatchRange(filter, size, maxLength);
        final int numBatches = store.numBatches();
        if (batchRange.getLastBatch() >= numBatches) {
            throw new IllegalStateException(String.format("Last batch index is %d, but maximum batch index is %d.",
                batchRange.getLastBatch(), numBatches - 1));
        }

        final var selection = createColumnSelection(filter, schema.numColumns());

        return new DefaultRowCursor(ColumnarCursorFactory.create(store, selection, batchRange), schema, selection);
    }

    private static BatchRange createBatchRange(final TableFilter filter, final long size, final int batchLength) {
        final long maxRowIndex = size - 1;
        // filter.getFromRowIndex() is guaranteed to return a value >= 0
        final long fromRowIndex = filter != null ? TableFilterUtils.extractFromIndex(filter) : 0L;
        // filter.getToRowIndex() is guaranteed to return a value >= fromRowIndex >= 0
        final long toRowIndex = filter != null ? TableFilterUtils.extractToIndex(filter, size) : maxRowIndex;

        final int firstBatchIndex = (int)(fromRowIndex / batchLength);
        final int lastBatchIndex = (int)(toRowIndex / batchLength);
        final int firstIndexInFirstBatch = (int)(fromRowIndex % batchLength);
        final int lastIndexInLastBatch = (int)(toRowIndex % batchLength);
        return new DefaultBatchRange(firstBatchIndex, firstIndexInFirstBatch, lastBatchIndex, lastIndexInLastBatch);
    }

    private static ColumnSelection createColumnSelection(final TableFilter filter, final int numColumns) {
        if (filter == null) {
            return new DefaultColumnSelection(numColumns);
        } else {
            if (TableFilterUtils.definesColumnFilter(filter)) {
                return new FilteredColumnSelection(numColumns,
                    TableFilterUtils.extractPhysicalColumnIndices(filter, numColumns));
            } else {
                return new DefaultColumnSelection(numColumns);
            }
        }
    }

    private ColumnarRowCursorFactory() {
    }

}
