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

import org.knime.core.columnar.ColumnDataIndex;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.schema.ColumnarWriteValueFactory;
import org.knime.core.data.v2.RowKeyWriteValue;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.data.v2.WriteValue;

final class ColumnarRowWriteCursor implements RowWriteCursor, ColumnDataIndex, RowWrite {

    // the initial capacity (in number of held elements) of a single chunk
    // arrow has a minimum capacity of 2
    private static final int CAPACITY_INIT_DEF = 2;

    private static final String CAPACITY_INIT_PROPERTY = "knime.columnar.capacity.initial";

    private static final int CAPACITY_INIT = Integer.getInteger(CAPACITY_INIT_PROPERTY, CAPACITY_INIT_DEF);

    // the maximum capacity (in number of held elements) of a single chunk
    // subtract 750 since arrow rounds up to the next power of 2 anyways
    private static final int CAPACITY_MAX_DEF = (1 << 15) - 750; // 32,018

    private static final String CAPACITY_MAX_PROPERTY = "knime.columnar.capacity.max";

    private static final int CAPACITY_MAX = Integer.getInteger(CAPACITY_MAX_PROPERTY, CAPACITY_MAX_DEF);

    // the target size (in bytes) of a full batch
    private static final long BATCH_SIZE_TARGET_DEF = 1L << 26; // 64 MB

    private static final String BATCH_SIZE_TARGET_PROPERTY = "knime.columnar.batch.size.target";

    private static final long BATCH_SIZE_TARGET = Long.getLong(BATCH_SIZE_TARGET_PROPERTY, BATCH_SIZE_TARGET_DEF);

    private final ColumnDataFactory m_columnDataFactory;

    private final ColumnDataWriter m_writer;

    private final ColumnarWriteValueFactory<?>[] m_factories;

    private final WriteValue<?>[] m_values;

    private RowKeyWriteValue m_rowKeyValue;

    private WriteBatch m_currentBatch;

    private int m_currentMaxIndex;

    private int m_currentIndex;

    private long m_size = 0;

    private ColumnWriteData[] m_currentData;

    private boolean m_adjusting;

    ColumnarRowWriteCursor(final ColumnStore store, final ColumnarWriteValueFactory<?>[] factories) {
        m_columnDataFactory = store.getFactory();
        m_writer = store.getWriter();
        m_factories = factories;
        m_adjusting = true;
        m_values = new WriteValue[m_factories.length];

        switchToNextData();
        m_currentIndex = -1;
    }

    @Override
    public final RowWrite forward() {
        m_currentIndex++;
        if (m_currentIndex > m_currentMaxIndex) {
            switchToNextData();
        }
        return this;
    }

    @Override
    public final <W extends WriteValue<?>> W getWriteValue(final int index) {
        @SuppressWarnings("unchecked")
        final W value = (W)m_values[index + 1];
        return value;
    }

    @Override
    public final void setMissing(final int index) {
        m_currentData[index + 1].setMissing(m_currentIndex);
    }

    @Override
    public final void setFrom(final RowRead access) {
        setRowKey(access.getRowKey());
        for (int i = 1; i < m_values.length; i++) {
            m_values[i].setValue(access.getValue(i - 1));
        }
    }

    @Override
    public int getNumColumns() {
        return m_values.length - 1;
    }

    @Override
    public void setRowKey(final String rowKey) {
        m_rowKeyValue.setRowKey(rowKey);
    }

    @Override
    public void setRowKey(final RowKeyValue rowKey) {
        m_rowKeyValue.setRowKey(rowKey);
    }

    @Override
    public boolean canForward() {
        return true;
    }

    @Override
    public final void close() {
        if (m_currentBatch != null) {
            m_currentBatch.release();
            m_currentBatch = null;
        }
        try {
            m_writer.close();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public final int getIndex() {
        return m_currentIndex;
    }

    final long getSize() {
        return m_size;
    }

    final void finish() {
        writeCurrentBatch(m_currentIndex + 1);
        close();
    }

    private final void writeCurrentBatch(final int numValues) {
        if (m_currentBatch != null) {

            // handle empty tables (fwd was never called)
            final ReadBatch readBatch = m_currentBatch.close(numValues);
            try {
                m_writer.write(readBatch);
            } catch (final IOException e) {
                throw new IllegalStateException("Problem occurred when writing column data.", e);
            }
            readBatch.release();
            m_currentBatch = null;
            m_size += numValues;
            m_currentIndex = 0;
        }
    }

    private final void switchToNextData() {
        if (m_adjusting && m_currentBatch != null) {
            final int curCapacity = m_currentBatch.capacity();
            final long curBatchSize = m_currentBatch.sizeOf();

            final int newCapacity;
            if (curBatchSize > 0) {
                // we want to avoid too much serialization overhead for capacities > 100. 100 rows should give us a good estimate for the capacity, though.
                long factor = BATCH_SIZE_TARGET / curBatchSize;
                if (curCapacity <= 100) {
                    factor = Math.min(8, factor);
                }
                newCapacity = (int)Math.min(CAPACITY_MAX, curCapacity * factor); // can't exceed Integer.MAX_VALUE
            } else {
                newCapacity = CAPACITY_MAX;
            }

            if (curCapacity < newCapacity) { // if factor < 1, then curCapacity > newCapacity
                m_currentBatch.expand(newCapacity);
                m_currentMaxIndex = m_currentBatch.capacity() - 1;
                if (newCapacity >= CAPACITY_MAX) {
                    m_adjusting = false;
                }
                return;
            } else {
                m_adjusting = false;
            }
        }

        final int chunkSize = m_currentBatch == null ? CAPACITY_INIT : m_currentBatch.capacity();
        writeCurrentBatch(m_currentIndex);

        // TODO can we preload data?
        m_currentBatch = m_columnDataFactory.create(chunkSize);
        m_currentData = m_currentBatch.getUnsafe();
        updateWriteValues(m_currentBatch);
        m_currentMaxIndex = m_currentBatch.capacity() - 1;
    }

    private void updateWriteValues(final WriteBatch batch) {
        for (int i = 0; i < m_values.length; i++) {
            @SuppressWarnings("unchecked")
            final ColumnarWriteValueFactory<ColumnWriteData> cast =
                ((ColumnarWriteValueFactory<ColumnWriteData>)m_factories[i]);
            m_values[i] = cast.createWriteValue(batch.get(i), this);
        }
        m_rowKeyValue = (RowKeyWriteValue)m_values[0];
    }

}
