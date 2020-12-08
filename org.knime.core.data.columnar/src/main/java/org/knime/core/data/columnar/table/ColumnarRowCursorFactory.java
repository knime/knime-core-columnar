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

import java.util.Set;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.ColumnDataIndex;
import org.knime.core.data.columnar.schema.ColumnarReadValueFactory;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.columnar.table.ResourceLeakDetector.ResourceWithRelease;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyReadValue;
import org.knime.core.data.v2.RowRead;

/**
 * Columnar implementation of {@link RowCursor} for reading data from columnar table backend.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @since 4.3
 */
final class ColumnarRowCursorFactory {

    static RowCursor create(final ColumnReadStore store, final ColumnarValueSchema schema, final long fromRowIndex,
        final long toRowIndex, final Set<Finalizer> openCursorFinalizers) {
        return create(store, schema, fromRowIndex, toRowIndex, openCursorFinalizers, null);
    }

    @SuppressWarnings("resource")
    static RowCursor create(final ColumnReadStore store, final ColumnarValueSchema schema, final long fromRowIndex,
        final long toRowIndex, final Set<Finalizer> openCursorFinalizers, final int[] selection) {

        // TODO move entire TableFilter extraction / parsing into this static method and out of AbstractColumnarContainerTable?
        // TODO we may also want to validate fromRowIndex / toRowIndex with actual size of table?

        final BatchReader reader = store.createReader();
        try {
            final long numBatches = reader.numBatches();
            if (numBatches == 0) {
                reader.close();
                return new EmptyRowCursor(schema);
            } else if (numBatches == 1) {
                return new SingleBatchRowCursor(reader, schema, fromRowIndex, toRowIndex, selection,
                    openCursorFinalizers);
            } else {
                return new MultiBatchRowCursor(reader, schema, fromRowIndex, toRowIndex, selection,
                    openCursorFinalizers);
            }
        } catch (final Exception e) {
            // TODO Logging / Handling
            throw new RuntimeException(e);
        }
    }

    private static int[] addRowKeyIndexToSelection(final int[] selection) {
        final int[] colIndicesAsInt = new int[selection.length + 1];
        colIndicesAsInt[0] = 0;
        int i = 1;
        for (final int index : selection) {
            colIndicesAsInt[i++] = index + 1;
        }
        return colIndicesAsInt;
    }

    private static class EmptyRowCursor implements RowCursor {

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
        }

        @Override
        public int getNumColumns() {
            return m_schema.numColumns() - 1;
        }

    }

    private static class MultiBatchRowCursor implements RowCursor, RowRead, ColumnDataIndex {
        private final BatchReader m_reader;

        private final ResourceWithRelease m_readerRelease;

        private final int m_maxBatchIndex;

        private final int m_lastBatchMaxIndex;

        private final int[] m_selection;

        private final ColumnarValueSchema m_schema;

        private final ColumnarReadValueFactory<?>[] m_factories;

        private final Set<Finalizer> m_openCursorFinalizers;

        private Finalizer m_finalizer;

        private ReadBatch m_currentBatch;

        private ReadValue[] m_currentValues;

        private int m_currentBatchIndex;

        private int m_currentIndex;

        private int m_currentMaxIndex;

        private NullableReadData[] m_currentData;

        private MultiBatchRowCursor(final BatchReader reader, final ColumnarValueSchema schema, final long fromRowIndex,
            final long toRowIndex, final int[] selection, final Set<Finalizer> openCursorFinalizers) {
            m_selection = selection != null ? addRowKeyIndexToSelection(selection) : null;
            m_reader = reader;
            m_readerRelease = new ResourceWithRelease(m_reader);
            m_schema = schema;
            m_openCursorFinalizers = openCursorFinalizers;
            m_factories = m_schema.getReadValueFactories();

            try {
                final int maxLength = m_reader.maxLength();
                m_maxBatchIndex = (int)(toRowIndex / maxLength);

                // in the last chunk we only iterate until toRowIndex
                m_lastBatchMaxIndex = (int)(toRowIndex % maxLength);

                m_currentBatchIndex = (int)(fromRowIndex / maxLength) - 1;

                // start index
                m_currentIndex = (int)(fromRowIndex % maxLength) - 1;

                readNextBatch();
            } catch (final Exception e) {
                // TODO Logging / Handling
                throw new RuntimeException(e);
            }
        }

        @Override
        public RowRead forward() {
            if (++m_currentIndex > m_currentMaxIndex) {
                m_currentBatch.release();
                m_finalizer.close();
                m_openCursorFinalizers.remove(m_finalizer);
                readNextBatch();
                m_currentIndex = 0;
            }
            return this;
        }

        @Override
        public boolean canForward() {
            return m_currentIndex < m_currentMaxIndex || m_currentBatchIndex < m_maxBatchIndex;
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
            return m_currentData[index + 1].isMissing(m_currentIndex);
        }

        @Override
        public void close() {
            try {
                // Finalizer could have already been closed in AbstractColumnarContainerTable::clear
                if (!m_finalizer.isClosed()) {
                    m_finalizer.close();
                    if (m_currentBatch != null) {
                        m_currentBatch.release();
                    }
                    m_openCursorFinalizers.remove(m_finalizer);
                    m_reader.close();
                }
            } catch (final Exception e) {
                // TODO Logging
                throw new IllegalStateException("Exception while closing ColumnarRowCursor.", e);
            }
        }

        @Override
        public final int getIndex() {
            return m_currentIndex;
        }

        private void readNextBatch() {
            try {
                m_currentBatch = m_reader.readRetained(++m_currentBatchIndex);
                m_currentData = m_currentBatch.getUnsafe();
                m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this,
                    new ResourceWithRelease(m_currentBatch, ReadBatch::release), m_readerRelease);
                m_openCursorFinalizers.add(m_finalizer);
                if (m_selection == null) {
                    m_currentValues = create(m_currentBatch);
                } else {
                    m_currentValues = create(m_currentBatch, m_selection);
                }

                m_currentMaxIndex =
                    m_currentBatchIndex != m_maxBatchIndex ? m_currentBatch.length() - 1 : m_lastBatchMaxIndex;
            } catch (final Exception e) {
                throw new IllegalStateException("Problem when reading batch from store.", e);
            }
        }

        private ReadValue[] create(final ReadBatch batch) {
            final ReadValue[] values = new ReadValue[m_schema.numColumns()];
            for (int i = 0; i < values.length; i++) {
                @SuppressWarnings("unchecked")
                final ColumnarReadValueFactory<NullableReadData> cast =
                    ((ColumnarReadValueFactory<NullableReadData>)m_factories[i]);
                values[i] = cast.createReadValue(batch.get(i), this);
            }
            return values;
        }

        private final ReadValue[] create(final ReadBatch batch, final int[] selection) {
            final ReadValue[] values = new ReadValue[m_schema.numColumns()];
            for (int i = 0; i < selection.length; i++) {
                @SuppressWarnings("unchecked")
                final ColumnarReadValueFactory<NullableReadData> cast =
                    ((ColumnarReadValueFactory<NullableReadData>)m_factories[selection[i]]);
                values[selection[i]] = cast.createReadValue(batch.get(selection[i]), this);
            }
            return values;
        }
    }

    private static class SingleBatchRowCursor implements RowCursor, RowRead, ColumnDataIndex {

        private final BatchReader m_reader;

        private final ResourceWithRelease m_readerRelease;

        private final int[] m_selection;

        private final NullableReadData[] m_data;

        private final ColumnarValueSchema m_schema;

        private final Set<Finalizer> m_openCursorFinalizers;

        private final int m_maxIndex;

        private final RowKeyValue m_rowKeyValue;

        private final Finalizer m_finalizer;

        private final ReadBatch m_batch;

        private final ReadValue[] m_values;

        private int m_index;

        private SingleBatchRowCursor(final BatchReader reader, final ColumnarValueSchema schema,
            final long fromRowIndex, final long toRowIndex, final int[] selection,
            final Set<Finalizer> openCursorFinalizers) {
            m_selection = selection != null ? addRowKeyIndexToSelection(selection) : null;
            m_reader = reader;
            m_readerRelease = new ResourceWithRelease(m_reader);
            m_schema = schema;
            m_openCursorFinalizers = openCursorFinalizers;

            try {
                final int maxLength = m_reader.maxLength();

                // lastIndex
                m_maxIndex = (int)(toRowIndex % maxLength);

                // current index
                m_index = (int)(fromRowIndex % maxLength) - 1;

                m_batch = m_reader.readRetained(0);
                m_data = m_batch.getUnsafe();
                m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this,
                    new ResourceWithRelease(m_batch, ReadBatch::release), m_readerRelease);
                m_openCursorFinalizers.add(m_finalizer);
                m_values = new ReadValue[m_schema.numColumns()];

                final ColumnarReadValueFactory<?>[] factories = m_schema.getReadValueFactories();
                if (m_selection == null) {
                    for (int i = 0; i < m_values.length; i++) {
                        @SuppressWarnings("unchecked")
                        final ColumnarReadValueFactory<NullableReadData> cast =
                            ((ColumnarReadValueFactory<NullableReadData>)factories[i]);
                        m_values[i] = cast.createReadValue(m_batch.get(i), this);
                    }
                } else {
                    for (int i = 0; i < m_selection.length; i++) {
                        @SuppressWarnings("unchecked")
                        final ColumnarReadValueFactory<NullableReadData> cast =
                            ((ColumnarReadValueFactory<NullableReadData>)factories[m_selection[i]]);
                        m_values[m_selection[i]] = cast.createReadValue(m_batch.get(m_selection[i]), this);
                    }
                }
                m_rowKeyValue = (RowKeyValue)m_values[0];
            } catch (final Exception e) {
                throw new IllegalStateException("Problem when reading batch from store.", e);
            }
        }

        @Override
        public RowRead forward() {
            m_index++;
            return this;
        }

        @Override
        public boolean canForward() {
            return m_index < m_maxIndex;
        }

        @Override
        public void close() {
            try {
                // Finalizer could have already been closed in AbstractColumnarContainerTable::clear
                if (!m_finalizer.isClosed()) {
                    m_finalizer.close();
                    if (m_batch != null) {
                        m_batch.release();
                    }
                    m_openCursorFinalizers.remove(m_finalizer);
                    m_reader.close();
                }
            } catch (final Exception e) {
                // TODO Logging
                throw new IllegalStateException("Exception while closing SingleBatchRowCursor.", e);
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
}
