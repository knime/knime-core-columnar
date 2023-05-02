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
 *   Apr 12, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.CloseableDataRowIterable;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.RandomRowAccessible.RandomAccessCursor;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * {@link CloseableDataRowIterable} that is backed by a {@link RandomRowAccessible} and produces {@link DataRow
 * DataRows} that use random access to materialize {@link DataCell DataCells} on demand.<br>
 * Each column is backed by its own RandomAccessCursor in order to allow column-parallel processing.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarRandomAccessRowIterable implements CloseableDataRowIterable, Closeable {

    private final RandomRowAccessible m_rowAccessible;

    private final ColumnarValueSchema m_schema;

    /*
     * Ensures that any open resources are closed when this ColumnarRandomRowIterable is closed.
     */
    private final FinalizerTracker m_openCursorFinalizers = new FinalizerTracker();

    private final AtomicBoolean m_open = new AtomicBoolean(true);

    ColumnarRandomAccessRowIterable(final RandomRowAccessible rowAccessible, final ColumnarValueSchema schema) {
        m_rowAccessible = rowAccessible;
        m_schema = schema;
    }

    @SuppressWarnings("resource")
    ColumnarRandomAccessRowIterable(final BatchReadStore store, final long numRows, final ColumnarValueSchema schema) {
        m_schema = schema;
        m_rowAccessible = new ColumnarRandomRowAccessible(new AllColumnsRandomAccessBatchReadable(store), numRows,
            store.batchLength());
    }

    @Override
    public CloseableRowIterator iterator() {
        CheckUtils.checkState(m_open.get(),
            "Illegal attempt to create an iterator on an already closed ColumnarRandomAccessRowIterable.");
        return new ColumnarRandomAccessRowIterator(m_rowAccessible, m_rowAccessible.size(), m_schema);
    }

    @Override
    public void close() throws IOException {
        if (m_open.getAndSet(false)) {
            m_openCursorFinalizers.close();
        }
    }

    private final class ColumnarRandomAccessRowIterator extends CloseableRowIterator {

        private final ColumnCursor[] m_columnCursors;

        private final long m_numRows;

        private long m_currentRow;

        ColumnarRandomAccessRowIterator(final RandomRowAccessible rowAccessible, final long numRows,
            final ColumnarValueSchema schema) {
            m_columnCursors = new ColumnCursor[schema.numColumns()];
            Arrays.setAll(m_columnCursors, i -> new ColumnCursor(rowAccessible, schema, i));
            m_numRows = numRows;
        }

        @Override
        public boolean hasNext() {
            return m_currentRow < m_numRows;
        }

        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return new RandomAccessDataRow(m_currentRow++, m_columnCursors);//NOSONAR
        }

        @Override
        public void close() {
            // columnCursors are tracked by the parent so that rows can live as long as the underlying table exists
        }

    }

    private static final class RandomAccessDataRow implements DataRow {

        private final long m_row;

        private final ColumnCursor[] m_columnCursors;

        private RowKey m_key;

        private DataCell[] m_cells;

        RandomAccessDataRow(final long row, final ColumnCursor[] columnCursors) {
            m_row = row;
            m_columnCursors = columnCursors;
            m_cells = new DataCell[columnCursors.length - 1];
        }

        @Override
        public Iterator<DataCell> iterator() {
            return IntStream.range(0, getNumCells()).mapToObj(this::getCell).iterator();
        }

        @Override
        public int getNumCells() {
            return m_columnCursors.length - 1;
        }

        @Override
        public RowKey getKey() {
            if (m_key == null) {
                m_key = m_columnCursors[0].getKey(m_row);
            }
            return m_key;
        }

        @Override
        public DataCell getCell(final int index) {
            var cell = m_cells[index];
            if (cell == null) {
                cell = m_columnCursors[index + 1].getCell(m_row);
                m_cells[index] = cell;
            }
            return cell;
        }

    }

    private final class ColumnCursor implements Closeable {

        private final RandomAccessCursor m_cursor;

        private final ReadAccess m_readAccess;

        private final ReadValue m_readValue;

        /*
         * Ensures that the resources held by this ColumnCursor are closed once it is garbage collected after all
         * DataRows pointing to it have been garbage collected.
         */
        private final Finalizer m_finalizer;

        ColumnCursor(final RandomRowAccessible rowAccessible, final ColumnarValueSchema schema,
            final int columnarIndex) {
            m_cursor = rowAccessible.createCursor(Selection.all().retainColumns(columnarIndex));
            m_readAccess = m_cursor.access().getAccess(columnarIndex);
            m_readValue = schema.getValueFactory(columnarIndex).createReadValue(m_readAccess);
            m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this, m_cursor);
            m_openCursorFinalizers.add(m_finalizer);
        }

        synchronized RowKey getKey(final long row) {
            m_cursor.moveTo(row);
            var rowKeyReadValue = (RowKeyValue)m_readValue;
            return new RowKey(rowKeyReadValue.getString());
        }

        synchronized DataCell getCell(final long row) {
            m_cursor.moveTo(row);
            if (m_readAccess.isMissing()) {
                return DataType.getMissingCell();
            } else {
                return m_readValue.getDataCell();
            }
        }

        @Override
        public void close() throws IOException {
            m_cursor.close();
            m_finalizer.close();
        }

    }

    /**
     * Ensures that all cursors read full batches, so that these batches can be reused by the other cursors.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private static final class AllColumnsRandomAccessBatchReadable implements RandomAccessBatchReadable {

        private final RandomAccessBatchReadable m_delegate;

        AllColumnsRandomAccessBatchReadable(final RandomAccessBatchReadable delegate) {
            m_delegate = delegate;
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_delegate.getSchema();
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
        }

        @Override
        public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
            return m_delegate.createRandomAccessReader();
        }

    }

}
