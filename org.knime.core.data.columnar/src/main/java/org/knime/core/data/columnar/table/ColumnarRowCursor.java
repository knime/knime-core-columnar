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

import org.knime.core.columnar.ColumnDataIndex;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.phantom.CloseableCloser;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.schema.ColumnarReadValueFactory;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.v2.WrappedReadValue;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyReadValue;

/**
 * Columnar implementation of {@link RowCursor} for reading data from columnar table backend.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
class ColumnarRowCursor implements RowCursor, ColumnDataIndex {

    // effectively final (set in #create)
    private CloseableCloser m_closer;

    private final Set<CloseableCloser> m_openCursorCloseables;

    private final ColumnDataReader m_reader;

    private final int m_maxBatchIndex;

    private final int m_lastBatchMaxIndex;

    private final int[] m_selection;

    private final ColumnarValueSchema m_schema;

    private final ColumnarReadValueFactory<ColumnReadData>[] m_factories;

    private ReadBatch m_currentBatch;

    private ReadValue[] m_currentValues;

    private int m_currentBatchIndex;

    private int m_currentIndex;

    private int m_currentMaxIndex;

    static ColumnarRowCursor create(final ColumnReadStore store, final ColumnarValueSchema schema,
        final long fromRowIndex, final long toRowIndex, final Set<CloseableCloser> openCursorCloseables) {
        return create(store, schema, fromRowIndex, toRowIndex, openCursorCloseables, null);
    }

    static ColumnarRowCursor create(final ColumnReadStore store, final ColumnarValueSchema schema,
        final long fromRowIndex, final long toRowIndex, final Set<CloseableCloser> openCursorCloseables,
        int[] selection) {
        if (selection != null) {
            selection = addRowKeyIndexToSelection(selection);
        }
        final ColumnarRowCursor cursor =
            new ColumnarRowCursor(store, schema, fromRowIndex, toRowIndex, openCursorCloseables, selection);
        cursor.m_closer = new CloseableCloser(cursor);
        openCursorCloseables.add(cursor.m_closer);
        return cursor;
    }

    private ColumnarRowCursor(final ColumnReadStore store, final ColumnarValueSchema schema, final long fromRowIndex,
        final long toRowIndex, final Set<CloseableCloser> openCursorCloseables, final int[] selection) {
        // TODO check that from fromRowIndex > 0 and <= toRowIndex
        // check that toRowIndex < table size (which currently cannot be determined)

        m_selection = selection;
        m_reader = store.createReader();
        m_schema = schema;
        m_openCursorCloseables = openCursorCloseables;

        // initialize ReadValue factories
        @SuppressWarnings("unchecked")
        final ColumnarReadValueFactory<ColumnReadData>[] factories =
            new ColumnarReadValueFactory[m_schema.getNumColumns()];
        for (int j = 0; j < factories.length; j++) {
            factories[j] = m_schema.getReadValueFactoryAt(j);
        }
        m_factories = factories;

        // number of chunks
        final int maxLength;
        try {
            maxLength = m_reader.getMaxLength();
        } catch (final Exception e) {
            // TODO
            throw new RuntimeException(e);
        }
        if (maxLength < 1) {
            m_maxBatchIndex = m_lastBatchMaxIndex = m_currentBatchIndex = m_currentIndex = m_currentMaxIndex = -1;
        } else {
            m_maxBatchIndex = (int)(toRowIndex / maxLength);

            // in the last chunk we only iterate until toRowIndex
            m_lastBatchMaxIndex = (int)(toRowIndex % maxLength);

            m_currentBatchIndex = (int)(fromRowIndex / maxLength) - 1;

            // start index
            m_currentIndex = (int)(fromRowIndex % maxLength) - 1;

            // read next batch
            readNextBatch();
        }
    }

    @Override
    public boolean poll() {
        // TODO throw appropriate exception in case canPoll = false but poll()
        // called
        if (++m_currentIndex > m_currentMaxIndex) {
            m_currentBatch.release();
            readNextBatch();
            m_currentIndex = 0;
        }
        return canPoll();
    }

    @Override
    public boolean canPoll() {
        return m_currentIndex < m_currentMaxIndex || m_currentBatchIndex < m_maxBatchIndex;
    }

    @Override
    public RowKeyValue getRowKeyValue() {
        return (RowKeyReadValue)m_currentValues[0];
    }

    @Override
    public int getNumColumns() {
        return m_schema.getNumColumns() - 1;
    }

    @Override
    public <V extends DataValue> V getValue(final int index) {
        /*
         * TODO performance - instanceof check
         *
         * Option 1: bitset to identify DataCellValueFactories
         * Option 3: Supplier<DataValue>[...] instead if ReadValue[...] and then get().
         *
         * Additional (later): dedicated cursor implementations for schemas without DataCellReadValues
         */
        final ReadValue value = m_currentValues[index + 1];
        if (!(value instanceof WrappedReadValue)) {
            @SuppressWarnings("unchecked")
            final V cast = (V)value;
            return cast;
        } else {
            @SuppressWarnings("unchecked")
            final V cast = (V)value.getDataCell();
            return cast;
        }
    }

    @Override
    public boolean isMissing(final int index) {
        // TODO CD - store currentData.
        return m_currentBatch.getUnsafe(index + 1).isMissing(m_currentIndex);
    }

    @Override
    public void close() {
        try {
            if (m_currentBatch != null) {
                m_currentBatch.release();
                m_currentBatch = null;
            }
            m_closer.close();
            m_openCursorCloseables.remove(m_closer);
            m_reader.close();
        } catch (final Exception e) {
            // TODO Logging
            throw new IllegalStateException("Exception while closing RowWriteCursor.", e);
        }
    }

    @Override
    public final int getIndex() {
        return m_currentIndex;
    }

    private void readNextBatch() {
        try {
            m_currentBatch = m_reader.readRetained(++m_currentBatchIndex);
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
        final ReadValue[] values = new ReadValue[m_schema.getNumColumns()];
        for (int i = 0; i < values.length; i++) {
            values[i] = m_factories[i].createReadValue(batch.get(i), this);
        }
        return values;
    }

    private final ReadValue[] create(final ReadBatch batch, final int[] selection) {
        final ReadValue[] values = new ReadValue[m_schema.getNumColumns()];
        for (int i = 0; i < selection.length; i++) {
            values[selection[i]] = m_factories[selection[i]].createReadValue(batch.get(selection[i]), this);
        }
        return values;
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
}
