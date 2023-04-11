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
 *   Oct 13, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.parallel.write;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.parallel.exec.DataWriter;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.DataSpec;

import com.google.common.base.Preconditions;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class AsyncBatchWriter implements GridWriter<WriteAccess> {

    public interface CapacityStrategy {
        int getInitialCapacity();

        /**
         * @param batch to potentially expand
         * @return the new capacity for the batch (or the same if the batch should not be expanded)
         */
        // TODO make this Batch<?> in order to discourage expanding the batch
        int getNewCapacity(WriteBatch batch);

    }

    /**
     * Always recommends the same strategy.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class FixedCapacityStrategy implements CapacityStrategy {

        private final int m_capacity;

        public FixedCapacityStrategy(final int capacity) {
            Preconditions.checkArgument(capacity > 0, "The capacity must be greater than 0.");
            m_capacity = capacity;
        }

        @Override
        public int getInitialCapacity() {
            return m_capacity;
        }

        @Override
        public int getNewCapacity(final WriteBatch batch) {
            return m_capacity;
        }

    }

    public static final class AdjustingCapacityStrategy implements CapacityStrategy {

        private boolean m_adjusting = true;

        private int m_currentCapacity;

        private final int m_initialCapacity;

        private final long m_targetBatchSize;

        private final int m_maxCapacity;

        public AdjustingCapacityStrategy(final int initialCapacity, final long targetBatchSize, final int maxCapacity) {
            m_initialCapacity = initialCapacity;
            m_currentCapacity = initialCapacity;
            m_targetBatchSize = targetBatchSize;
            m_maxCapacity = maxCapacity;
        }

        @Override
        public int getInitialCapacity() {
            return m_initialCapacity;
        }

        @Override
        public int getNewCapacity(final WriteBatch batch) {
            int curCapacity = batch.capacity();
            if (m_adjusting) {
                final int newCapacity = recalculateCapacity(batch, curCapacity);
                // TODO why do curCapacity and m_batchCapacity differ?
                if (curCapacity < newCapacity) {
                    if (newCapacity == m_maxCapacity) {
                        m_adjusting = false;
                    }
                    return newCapacity;
                } else {
                    m_adjusting = false;
                }
            }
            return curCapacity;
        }

        private int recalculateCapacity(final WriteBatch batch, final int curCapacity) {
            final long curBatchSize = batch.sizeOf();

            if (curBatchSize > 0) {
                // we want to avoid too much serialization overhead for capacities > 100
                // 100 rows should give us a good estimate for the capacity, though
                long factor = m_targetBatchSize / curBatchSize;
                if (curCapacity <= 100) {
                    factor = Math.min(8, factor);
                }
                return (int)Math.min(m_maxCapacity, curCapacity * factor); // can't exceed Integer.MAX_VALUE
            } else {
                return m_maxCapacity;
            }
        }

    }


    private final BatchWriter m_writer;

    private WriteBatch m_currentBatch;

    private final AtomicInteger m_finalizeBatchBarrier;

    private final AtomicInteger m_batchCapacity;

    private final CapacityStrategy m_capacityStrategy;

    private final AsyncDataWriter[] m_columnWriters;


    public AsyncBatchWriter(final BatchStore batchStore, final CapacityStrategy capacityStrategy) {
        m_capacityStrategy = capacityStrategy;
        m_batchCapacity = new AtomicInteger(capacityStrategy.getInitialCapacity());
        m_writer = batchStore.getWriter();
        final var schema = batchStore.getSchema();
        m_finalizeBatchBarrier = new AtomicInteger(schema.numColumns());
        m_columnWriters = schema.specStream()//
            .map(AsyncDataWriter::new)//
            .toArray(AsyncDataWriter[]::new);
        startBatch();
    }

    @Override
    public DataWriter<WriteAccess> getDataWriter(final int colIdx) {
        return m_columnWriters[colIdx];
    }

    @Override
    public int numWriters() {
        return m_columnWriters.length;
    }

    private void switchBatch() {
        var newCapacity = m_capacityStrategy.getNewCapacity(m_currentBatch);
        if (newCapacity > m_currentBatch.capacity()) {
            m_currentBatch.expand(newCapacity);
            m_finalizeBatchBarrier.set(m_columnWriters.length);
            m_batchCapacity.set(m_currentBatch.capacity());
        } else {
            finishBatch(m_batchCapacity.get());
            startBatch();
        }
    }

    private void startBatch() {
        m_currentBatch = m_writer.create(m_batchCapacity.get());
        resetColumnWriters();
    }

    private void finishBatch(final int batchLength) {
        assert allColumnWritersHaveExpectedIndex(batchLength) : "The column writers are out of sync.";
        var finishedBatch = m_currentBatch.close(batchLength);
        m_currentBatch = null;
        try {
            m_writer.write(finishedBatch);
        } catch (IOException ex) {
            // TODO we probably need a more elaborate mechanism to properly work in the concurrent setting
            throw new IllegalStateException("Failed to write batch.", ex);
        }
        finishedBatch.release();
    }

    @Override
    public void flush() throws IOException {
        var currentBatchLength = getCurrentBatchLength();
        if (currentBatchLength > 0) {
            finishBatch(currentBatchLength);
            startBatch();
        }
    }

    private int getCurrentBatchLength() {
        return m_columnWriters[0].getIndex();
    }

    private boolean allColumnWritersHaveExpectedIndex(final int expected) {
        return Stream.of(m_columnWriters).mapToInt(AsyncDataWriter::getIndex).allMatch(i -> i == expected);
    }

    private void resetColumnWriters() {
        int numColumns = m_currentBatch.numData();
        m_finalizeBatchBarrier.set(numColumns);
        for (int i = 0; i < numColumns; i++) {
            m_columnWriters[i].reset(m_currentBatch.get(i));
        }

    }

    @Override
    public void close() throws IOException {
        // TODO how to tell the column writers that we are done?
        if (m_currentBatch != null) {
            m_currentBatch.release();
            m_currentBatch = null;
        }
    }

    private final class AsyncDataWriter implements DataWriter<WriteAccess>, ColumnDataIndex {

        private volatile int m_idx = 0;

        // non-atomic, non-volatile copy of m_batchCapacity
        private int m_capacity;

        private final ColumnarWriteAccess m_writeAccess;

        AsyncDataWriter(final DataSpec spec) {
            m_writeAccess = ColumnarAccessFactoryMapper.createAccessFactory(spec).createWriteAccess(this);
        }

        void reset(final NullableWriteData writeData) {
            m_writeAccess.setData(writeData);
            m_idx = 0;
            m_capacity = m_batchCapacity.get();
        }

        @Override
        public WriteAccess getAccess() {
            return m_writeAccess;
        }

        @Override
        public boolean canWrite() {
            // assuming m_batchCapacity goes up, only update our local
            // non-volatile copy if we would exceed the last-known value.
            if ( m_idx >= m_capacity ) {
                m_capacity = m_batchCapacity.get();
            }
            return m_idx < m_capacity;
//            return m_idx < m_batchCapacity.get();
        }

        // assumption: advance() is not called if canWrite()==false
        @Override
        public void advance() {
            ++m_idx;
            if (!canWrite() && m_finalizeBatchBarrier.decrementAndGet() == 0) {
                // this is the last column to finish writing in this batch
                switchBatch();
            }
        }

        @Override
        public int getIndex() {
            return m_idx;
        }
    }

}