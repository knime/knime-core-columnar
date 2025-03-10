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
 *   Apr 23, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cursor;

import static org.knime.core.columnar.access.ColumnarAccessFactoryMapper.createWriteAccesses;

import java.io.IOException;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.badger.BatchingWritable;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Static factory class for {@link WriteCursor WriteCursors} that write to a {@link BatchStore}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarWriteCursorFactory {

    // the maximum capacity (in number of held elements) of a single chunk
    // subtract 750 since arrow rounds up to the next power of 2 anyways
    static final int CAPACITY_MAX_DEF = (1 << 15) - 750; // 32,018

    private ColumnarWriteCursorFactory() {
        // static factory class
    }

    /**
     * Creates a {@link WriteCursor} that allows to write into the provided {@link BatchStore} in a row-wise fashion via
     * a {@link WriteAccessRow}.<br>
     * <b>Note:</b> BatchStores are currently limited to a single writer, see {@link BatchWritable#getWriter()}.
     *
     * @param store the underlying storage
     * @return the {@link WriteCursor}
     */
    public static ColumnarWriteCursor createWriteCursor(final BatchWritable store) {
        if (store instanceof BatchingWritable batchingStore) {
            return batchingStore.getBatchingWriteCursor();
        } else {
            return new ColumnarWriteCursorImpl(store);
        }
    }

    /**
     * A {@link WriteCursor} that is backed by a {@link BatchWriter} and tracks the number of times {@link #commit()} is
     * called.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface ColumnarWriteCursor extends WriteCursor<WriteAccessRow> {

        /**
         * Get the number of rows that have been written to this cursor.
         *
         * @return the number of rows written
         */
        long numRows();
    }

    /**
     * Columnar implementation of {@link RowWriteCursor} for writing data to a columnar table backend.
     *
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    private static final class ColumnarWriteCursorImpl implements ColumnarWriteCursor, ColumnDataIndex, WriteAccessRow {

        // the initial capacity (in number of held elements) of a single chunk
        // arrow has a minimum capacity of 2
        private static final int CAPACITY_INIT_DEF = 2;

        private static final String CAPACITY_INIT_PROPERTY = "knime.columnar.capacity.initial";

        private static final int CAPACITY_INIT = Integer.getInteger(CAPACITY_INIT_PROPERTY, CAPACITY_INIT_DEF);

        private static final String CAPACITY_MAX_PROPERTY = "knime.columnar.capacity.max";

        private static final int CAPACITY_MAX = Integer.getInteger(CAPACITY_MAX_PROPERTY, CAPACITY_MAX_DEF);

        // the target size (in bytes) of a full batch
        private static final long BATCH_SIZE_TARGET_DEF = 1L << 26; // 64 MB

        private static final String BATCH_SIZE_TARGET_PROPERTY = "knime.columnar.batch.size.target";

        private static final long BATCH_SIZE_TARGET = Long.getLong(BATCH_SIZE_TARGET_PROPERTY, BATCH_SIZE_TARGET_DEF);

        private final BatchWriter m_writer;

        private WriteBatch m_currentBatch;

        private int m_currentMaxIndex;

        private int m_currentIndex;

        private final ColumnarWriteAccess[] m_accesses;

        private boolean m_adjusting;

        private long m_numRows;

        ColumnarWriteCursorImpl(final BatchWritable store) {
            m_writer = store.getWriter();
            m_adjusting = true;

            final ColumnarSchema schema = store.getSchema();
            m_accesses = createWriteAccesses(schema.specStream(), this);
            switchToNextData();
        }

        @Override
        public void commit() throws IOException {
            m_numRows++;
            m_currentIndex++;
            if (m_currentIndex > m_currentMaxIndex) {
                switchToNextData();
            }
        }

        @Override
        public long numRows() {
            return m_numRows;
        }

        @Override
        public int size() {
            return m_accesses.length;
        }

        @Override
        public final void close() throws IOException {
            if (m_currentBatch != null) {
                m_currentBatch.release();
                m_currentBatch = null;
            }
            m_writer.close();
        }

        @Override
        public final int getIndex() {
            return m_currentIndex;
        }

        @Override
        public void flush() throws IOException {
            throw new UnsupportedOperationException("TODO: ColumnarWriteCursorImpl.flush() should probably not be called?");
            // TODO (TP): This was implemented before as
            //   writeCurrentBatch(m_currentIndex + 1);
            //   If we do this, then that would set m_currentBatch = null, but not switch to a new batch or prepare for that in any way.
            //   So basically, after flush() no further rows can be written, which is surely not intended.
        }

        @Override
        public void finish() throws IOException {
            writeCurrentBatch(m_currentIndex);
            // TODO: As per contract, this should block until the data is written to disk.
            // But right now we don't do this.
        }

        private void writeCurrentBatch(final int numValues) {
            if (m_currentBatch != null) {

                // handle empty tables (fwd was never called)
                final ReadBatch readBatch = m_currentBatch.close(Math.max(numValues, 0));
                try {
                    m_writer.write(readBatch);
                } catch (final IOException e) {
                    throw new IllegalStateException("Problem occurred when writing column data.", e);
                } finally {
                    readBatch.release();
                    m_currentBatch = null;
                    m_currentIndex = 0;
                }
            }
        }

        private void switchToNextData() {
            if (m_adjusting && m_currentBatch != null) {
                final int curCapacity = m_currentBatch.capacity();
                final long curBatchSize = m_currentBatch.sizeOf();

                final int newCapacity;
                if (curBatchSize > 0) {
                    // we want to avoid too much serialization overhead for capacities > 100
                    // 100 rows should give us a good estimate for the capacity, though
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

            m_currentBatch = m_writer.create(chunkSize);
            updateWriteValues(m_currentBatch);
            m_currentMaxIndex = m_currentBatch.capacity() - 1;
        }

        private void updateWriteValues(final WriteBatch batch) {
            for (int i = 0; i < m_accesses.length; i++) {
                m_accesses[i].setData(batch.get(i));
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends WriteAccess> A getWriteAccess(final int index) {
            return (A)m_accesses[index];
        }

        @Override
        public void setFrom(final ReadAccessRow row) {
            for (int i = 0; i < m_accesses.length; i++) {
                m_accesses[i].setFrom(row.getAccess(i));
            }
        }

        @Override
        public WriteAccessRow access() {
            return this;
        }
    }

}
