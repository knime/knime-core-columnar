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

import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.ColumnDataIndex;
import org.knime.core.data.columnar.schema.ColumnarReadValueFactory;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.columnar.table.ResourceLeakDetector.ResourceWithRelease;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyReadValue;
import org.knime.core.data.v2.RowRead;
import org.knime.core.node.NodeLogger;

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

    private static final class SingleBatchRowCursor implements RowCursor, RowRead, ColumnDataIndex {

        private final NullableReadData[] m_data;

        private final ColumnarValueSchema m_schema;

        private final Set<Finalizer> m_openCursorFinalizers;

        private final int m_maxIndex;

        private final RowKeyValue m_rowKeyValue;

        private final ReadBatch m_batch;

        private final ReadValue[] m_values;

        // effectively final
        private Finalizer m_finalizer;

        private int m_index;

        private SingleBatchRowCursor(final ColumnReadStore store, final ColumnarValueSchema schema,
            final int batchIndex, final int firstIndexInBatch, final int lastIndexInBatch,
            final Set<Finalizer> openCursorFinalizers, final int[] selection) throws IOException {

            m_schema = schema;
            m_openCursorFinalizers = openCursorFinalizers;
            m_index = firstIndexInBatch;
            m_maxIndex = lastIndexInBatch;

            try (@SuppressWarnings("resource")
            final BatchReader reader = selection == null ? store.createReader()
                : store.createReader(new FilteredColumnSelection(schema.numColumns(), selection))) {
                m_batch = reader.readRetained(batchIndex);
            }
            m_data = m_batch.getUnsafe();
            m_values = selection == null ? createReadValues(m_batch, m_schema, this)
                : createReadValues(m_batch, m_schema, this, selection);
            m_rowKeyValue = (RowKeyValue)m_values[0];
        }

        @Override
        public RowRead forward() {
            m_index++;
            return m_index <= m_maxIndex ? this : null;
        }

        @Override
        public boolean canForward() {
            return m_index < m_maxIndex;
        }

        @Override
        public void close() {
            // Finalizer could have already been closed in AbstractColumnarContainerTable::clear
            if (!m_finalizer.isClosed()) {
                m_finalizer.close();
                m_batch.release();
                m_openCursorFinalizers.remove(m_finalizer);
            }
        }

        @Override
        public int getNumColumns() {
            return m_schema.numColumns() - 1;
        }

        @Override
        public <D extends DataValue> D getValue(final int index) {
            @SuppressWarnings("unchecked")
            final D cast = (D)m_values[index + 1];
            return cast;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_data[index + 1].isMissing(m_index);
        }

        @Override
        public int getIndex() {
            return m_index;
        }

        @Override
        public RowKeyValue getRowKey() {
            return m_rowKeyValue;
        }

    }

    private static final class MultiBatchRowCursor implements RowCursor, RowRead, ColumnDataIndex {

        private final BatchReader m_reader;

        private final ResourceWithRelease m_readerRelease;

        private final int[] m_selection;

        private final ColumnarValueSchema m_schema;

        private final Set<Finalizer> m_openCursorFinalizers;

        private Finalizer m_finalizer;

        private ReadBatch m_currentBatch;

        private ReadValue[] m_currentValues;

        private final int m_lastBatchIndex;

        private final int m_lastIndexInLastBatch;

        private int m_currentBatchIndex;

        private int m_currentIndexInCurrentBatch;

        private int m_lastIndexInCurrentBatch;

        private NullableReadData[] m_currentData;

        private MultiBatchRowCursor(final ColumnReadStore store, final ColumnarValueSchema schema, // NOSONAR
            final int firstBatchIndex, final int lastBatchIndex, final int firstIndexInFirstBatch,
            final int lastIndexInLastBatch, final Set<Finalizer> openCursorFinalizers, final int[] selection) {
            m_selection = selection;
            @SuppressWarnings("resource")
            final BatchReader reader = selection == null ? store.createReader()
                : store.createReader(new FilteredColumnSelection(schema.numColumns(), selection));
            m_reader = reader;
            m_readerRelease = new ResourceWithRelease(m_reader);
            m_schema = schema;
            m_openCursorFinalizers = openCursorFinalizers;
            m_currentBatchIndex = firstBatchIndex;
            m_lastBatchIndex = lastBatchIndex;
            m_currentIndexInCurrentBatch = firstIndexInFirstBatch;
            m_lastIndexInLastBatch = lastIndexInLastBatch;
        }

        @Override
        public RowRead forward() {
            m_currentIndexInCurrentBatch++;
            if (m_currentIndexInCurrentBatch > m_lastIndexInCurrentBatch) {
                if (m_currentBatchIndex >= m_lastBatchIndex) {
                    return null;
                }
                m_currentBatch.release();
                m_finalizer.close();
                m_openCursorFinalizers.remove(m_finalizer);
                m_currentBatchIndex++;
                readCurrentBatch();
                m_currentIndexInCurrentBatch = 0;
            }
            return this;
        }

        @Override
        public boolean canForward() {
            return m_currentIndexInCurrentBatch < m_lastIndexInCurrentBatch || m_currentBatchIndex < m_lastBatchIndex;
        }

        @Override
        public RowKeyValue getRowKey() {
            return (RowKeyReadValue)m_currentValues[0];
        }

        @Override
        public int getNumColumns() {
            return m_schema.numColumns() - 1;
        }

        @Override
        public <V extends DataValue> V getValue(final int index) {
            @SuppressWarnings("unchecked")
            final V cast = (V)m_currentValues[index + 1];
            return cast;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_currentData[index + 1].isMissing(m_currentIndexInCurrentBatch);
        }

        private void closeReader() {
            try {
                m_reader.close();
            } catch (IOException e) {
                final String error = "Exception while closing ColumnarRowCursor.";
                LOGGER.error(error, e);
                throw new IllegalStateException(error, e);
            }
        }

        @Override
        public void close() {
            // Finalizer could have already been closed in AbstractColumnarContainerTable::clear
            if (!m_finalizer.isClosed()) {
                m_finalizer.close();
                m_currentBatch.release();
                m_openCursorFinalizers.remove(m_finalizer);
                closeReader();
            }
        }

        @Override
        public final int getIndex() {
            return m_currentIndexInCurrentBatch;
        }

        private void readCurrentBatch() {
            try {
                m_currentBatch = m_reader.readRetained(m_currentBatchIndex);
            } catch (IOException e) {
                final String error = "Exception while reading batch from store.";
                LOGGER.error(error, e);
                throw new IllegalStateException(error, e);
            }
            m_currentData = m_currentBatch.getUnsafe();
            m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this,
                new ResourceWithRelease(m_currentBatch, ReadBatch::release), m_readerRelease);
            m_openCursorFinalizers.add(m_finalizer);
            if (m_selection == null) {
                m_currentValues = createReadValues(m_currentBatch, m_schema, this);
            } else {
                m_currentValues = createReadValues(m_currentBatch, m_schema, this, m_selection);
            }

            if (m_currentBatchIndex != m_lastBatchIndex) {
                m_lastIndexInCurrentBatch = m_currentBatch.length() - 1;
            } else {
                m_lastIndexInCurrentBatch = m_lastIndexInLastBatch;
                closeReader();
            }
        }

    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarRowCursorFactory.class);

    static RowCursor create(final ColumnReadStore store, final ColumnarValueSchema schema, final long size,
        final Set<Finalizer> openCursorFinalizers) throws IOException {
        return create(store, schema, size, openCursorFinalizers, null);
    }

    static RowCursor create(final ColumnReadStore store, final ColumnarValueSchema schema, final long size, // NOSONAR
        final Set<Finalizer> openCursorFinalizers, final TableFilter filter) throws IOException {

        final long maxRowIndex = size - 1;
        // filter.getFromRowIndex() is guaranteed to return a value >= 0
        final long fromRowIndex = filter != null ? filter.getFromRowIndex().orElse(0L) : 0L;
        // filter.getToRowIndex() is guaranteed to return a value >= fromRowIndex >= 0
        final long toRowIndex = filter != null ? filter.getToRowIndex().orElse(maxRowIndex) : maxRowIndex;

        if (size < 1) {
            return new EmptyRowCursor(schema);
        }

        final Optional<Set<Integer>> materializeColumnIndices =
            filter != null ? filter.getMaterializeColumnIndices() : Optional.empty();
        final int[] selection = materializeColumnIndices.isPresent() ? IntStream
            // prepend index of row key column to selection
            .concat(IntStream.of(0), materializeColumnIndices.get().stream().sorted().mapToInt(i -> i.intValue() + 1))
            .toArray() : null;

        final int maxLength = store.maxLength();
        if (maxLength < 1) {
            throw new IllegalStateException(
                String.format("Length of table is %d, but maximum batch length is %d.", size, maxLength));
        }

        final int firstBatchIndex = (int)(fromRowIndex / maxLength);
        final int lastBatchIndex = (int)(toRowIndex / maxLength);
        // we eventually have to forward once to access the first element (i.e., the element at fromRowIndex)
        // therefore, the first index in the first batch must be set to the the index of the first element minus 1
        final int firstIndexInFirstBatch = (int)(fromRowIndex % maxLength) - 1;
        final int lastIndexInLastBatch = (int)(toRowIndex % maxLength);

        final int numBatches = store.numBatches();
        if (lastBatchIndex >= numBatches) {
            throw new IllegalStateException(String.format("Last batch index is %d, but maximum batch index is %d.",
                lastBatchIndex, numBatches - 1));
        }

        if (firstBatchIndex == lastBatchIndex) {
            final SingleBatchRowCursor cursor = new SingleBatchRowCursor(store, schema, firstBatchIndex,
                firstIndexInFirstBatch, lastIndexInLastBatch, openCursorFinalizers, selection);
            cursor.m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(cursor,
                new ResourceWithRelease(cursor.m_batch, ReferencedData::release));
            openCursorFinalizers.add(cursor.m_finalizer);
            return cursor;
        } else {
            final MultiBatchRowCursor cursor = new MultiBatchRowCursor(store, schema, firstBatchIndex, // NOSONAR
                lastBatchIndex, firstIndexInFirstBatch, lastIndexInLastBatch, openCursorFinalizers, selection);
            // can't invoke this in the constructor since it passes a reference to itself to the ResourceLeakDetector
            cursor.readCurrentBatch();
            return cursor;
        }
    }

    private static ReadValue[] createReadValues(final ReadBatch batch, final ColumnarValueSchema schema,
        final ColumnDataIndex index) {
        final ColumnarReadValueFactory<?>[] factories = schema.getReadValueFactories();
        final ReadValue[] values = new ReadValue[schema.numColumns()];
        for (int i = 0; i < values.length; i++) {
            @SuppressWarnings("unchecked")
            final ColumnarReadValueFactory<NullableReadData> cast =
                ((ColumnarReadValueFactory<NullableReadData>)factories[i]);
            values[i] = cast.createReadValue(batch.get(i), index);
        }
        return values;
    }

    private static final ReadValue[] createReadValues(final ReadBatch batch, final ColumnarValueSchema schema,
        final ColumnDataIndex index, final int[] selection) {
        final ColumnarReadValueFactory<?>[] factories = schema.getReadValueFactories();
        final ReadValue[] values = new ReadValue[schema.numColumns()];
        for (int i = 0; i < selection.length; i++) {
            @SuppressWarnings("unchecked")
            final ColumnarReadValueFactory<NullableReadData> cast =
                ((ColumnarReadValueFactory<NullableReadData>)factories[selection[i]]);
            values[selection[i]] = cast.createReadValue(batch.get(selection[i]), index);
        }
        return values;
    }

    private ColumnarRowCursorFactory() {
    }

}
