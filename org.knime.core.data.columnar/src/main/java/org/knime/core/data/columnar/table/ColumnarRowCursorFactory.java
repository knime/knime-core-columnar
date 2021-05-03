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
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.knime.core.columnar.cursor.ColumnarCursorFactory;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyReadValue;
import org.knime.core.data.v2.RowRead;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;

/**
 * Columnar implementations of {@link RowCursor} for reading data from columnar table backend.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class ColumnarRowCursorFactory {

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

    private static final class DefaultRowCursor implements RowCursor, RowRead {

        private final LookaheadCursor<ReadAccessRow> m_delegate;

        private final ReadValue[] m_values;

        private final ReadAccess[] m_accesses;

        private final ColumnarValueSchema m_schema;

        private final Set<Finalizer> m_openCursorFinalizers;

        // effectively final
        private Finalizer m_finalizer;

        private DefaultRowCursor(final LookaheadCursor<ReadAccessRow> delegate,
            final Set<Finalizer> openCursorFinalizers, final ColumnarValueSchema schema) throws IOException {
            m_delegate = delegate;
            m_schema = schema;
            ReadAccessRow access = delegate.access();
            m_accesses = IntStream.range(0, access.getNumColumns())//
                .mapToObj(access::getAccess)//
                .toArray(ReadAccess[]::new);
            final var facs = schema.getValueFactories();
            m_values = IntStream.range(0, access.getNumColumns())
                .mapToObj(i -> facs[i].createReadValue(access.getAccess(i))).toArray(ReadValue[]::new);
            m_openCursorFinalizers = openCursorFinalizers;
        }

        @Override
        public boolean canForward() {
            return m_delegate.canForward();
        }

        @Override
        public RowRead forward() {
            return m_delegate.forward() ? this : null;
        }

        @Override
        public int getNumColumns() {
            return m_schema.numColumns() - 1;
        }

        @Override
        public void close() {
            // Finalizer could have already been closed in AbstractColumnarContainerTable::clear
            if (!m_finalizer.isClosed()) {
                m_finalizer.close();
                m_delegate.close();
                m_openCursorFinalizers.remove(m_finalizer);
            }
        }

        @Override
        public <D extends DataValue> D getValue(final int index) {
            @SuppressWarnings("unchecked")
            final D cast = (D)m_values[index + 1];
            return cast;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_accesses[index + 1].isMissing();
        }

        @Override
        public RowKeyValue getRowKey() {
            return (RowKeyReadValue)m_values[0];
        }

    }

    static RowCursor create(final BatchReadStore store, final ColumnarValueSchema schema, final long size,
        final Set<Finalizer> openCursorFinalizers) throws IOException {
        return create(store, schema, size, openCursorFinalizers, null);
    }

    static RowCursor create(final BatchReadStore store, final ColumnarValueSchema schema, final long size, // NOSONAR
        final Set<Finalizer> openCursorFinalizers, final TableFilter filter) throws IOException {

        final long maxRowIndex = size - 1;
        // filter.getFromRowIndex() is guaranteed to return a value >= 0
        final long fromRowIndex = filter != null ? filter.getFromRowIndex().orElse(0L) : 0L;
        // filter.getToRowIndex() is guaranteed to return a value >= fromRowIndex >= 0
        final long toRowIndex = filter != null ? filter.getToRowIndex().orElse(maxRowIndex) : maxRowIndex;

        if (size < 1) {
            return new EmptyRowCursor(schema);
        }

        final ColumnSelection selection = createColumnSelection(filter, schema.numColumns());

        final int maxLength = store.batchLength();
        if (maxLength < 1) {
            throw new IllegalStateException(
                String.format("Length of table is %d, but maximum batch length is %d.", size, maxLength));
        }

        final int firstBatchIndex = (int)(fromRowIndex / maxLength);
        final int lastBatchIndex = (int)(toRowIndex / maxLength);
        final int firstIndexInFirstBatch = (int)(fromRowIndex % maxLength);
        final int lastIndexInLastBatch = (int)(toRowIndex % maxLength);

        final int numBatches = store.numBatches();
        if (lastBatchIndex >= numBatches) {
            throw new IllegalStateException(String.format("Last batch index is %d, but maximum batch index is %d.",
                lastBatchIndex, numBatches - 1));
        }

        var cursor = new DefaultRowCursor(ColumnarCursorFactory.create(store, selection, firstBatchIndex,
            lastBatchIndex, firstIndexInFirstBatch, lastIndexInLastBatch), openCursorFinalizers, schema);
        // can't invoke this in the constructor since it passes a reference to itself to the ResourceLeakDetector
        cursor.m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(cursor, cursor.m_delegate);
        openCursorFinalizers.add(cursor.m_finalizer);
        return cursor;
    }

    private static ColumnSelection createColumnSelection(final TableFilter filter, final int numColumns) {
        if (filter == null) {
            return new DefaultColumnSelection(numColumns);
        } else {
            final Optional<int[]> selection =
                filter.getMaterializeColumnIndices().map(ColumnarRowCursorFactory::translateMaterializedColumnIndices);
            if (selection.isPresent()) {
                return new FilteredColumnSelection(numColumns, selection.get());
            } else {
                return new DefaultColumnSelection(numColumns);
            }
        }
    }

    private static int[] translateMaterializedColumnIndices(final Set<Integer> materialized) {
        return IntStream.concat(
            // prepend index of row key column to selection
            IntStream.of(0), //
            materialized.stream()//
                .sorted()//
                .mapToInt(i -> i.intValue() + 1))// increment by one to accommodate row key column
            .toArray();
    }

    private ColumnarRowCursorFactory() {
    }

}
