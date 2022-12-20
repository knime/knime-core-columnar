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
 *   Oct 14, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.parallel.exec.ColumnTask;
import org.knime.core.columnar.parallel.exec.DataWriter;
import org.knime.core.columnar.parallel.exec.RowTaskBatch;
import org.knime.core.columnar.parallel.exec.WriteTaskExecutor;
import org.knime.core.columnar.parallel.write.GridWriter;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowKeyReadValue;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.table.access.ReadAccess;

import com.google.common.collect.Iterators;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class PrefetchingBatchRowIterator extends CloseableRowIterator {

    private final WriteTaskExecutor<RowAccess> m_taskExecutor;

    private final BlockingQueue<RowBatch> m_rowBatchQueue;

    private final BlockingQueue<MiniReadBatch> m_miniBatchQueue;

    private final RowBatchBuilder m_batchBuilder;

    private final MiniBatchDispatcher m_miniBatchDispatcher;

    private final Future<?> m_dispatchFuture;

    private final ExecutorService m_executor;

    private Iterator<DataRow> m_currentBatchIterator = Collections.emptyIterator();

    private int m_consumedBatches;

    PrefetchingBatchRowIterator(final ColumnarContainerTable table, final int numPendingBatches, final int batchSize,
        final ExecutorService executor) {
        m_executor = executor;
        var schema = table.getSchema();
        m_rowBatchQueue = new ArrayBlockingQueue<>(numPendingBatches);
        m_batchBuilder = new RowBatchBuilder(batchSize, schema.numColumns());
        m_miniBatchQueue = new ArrayBlockingQueue<>(numPendingBatches);
        m_miniBatchDispatcher = new MiniBatchDispatcher(table.getStore(), schema, batchSize);
        m_dispatchFuture = m_executor.submit(m_miniBatchDispatcher);
        m_taskExecutor = new WriteTaskExecutor<>(m_batchBuilder, executor, MiniReadBatch.NULL);
    }

    @Override
    public void close() {
        m_dispatchFuture.cancel(true);
        m_miniBatchDispatcher.close();
        m_taskExecutor.close();
    }

    @Override
    public boolean hasNext() {
        if (!m_currentBatchIterator.hasNext() && m_miniBatchDispatcher.moreBatchesOnTheWay(m_consumedBatches)) {
            switchRowBatch();
        }
        return m_currentBatchIterator.hasNext();
    }

    private void switchRowBatch() {
        try {
            m_currentBatchIterator = m_rowBatchQueue.take().iterator();
            m_consumedBatches++;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the next RowBatch.", ex);
        }
    }

    @Override
    public DataRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        } else {
            return m_currentBatchIterator.next();
        }
    }

    private final class MiniBatchDispatcher implements AutoCloseable, Runnable {

        private final RandomAccessBatchReader m_batchReader;

        private final int m_numBatches;

        private final int m_miniBatchSize;

        private final ColumnarValueSchema m_schema;

        private final AtomicLong m_dispatchedBatches = new AtomicLong(0);

        private final AtomicBoolean m_running = new AtomicBoolean(true);

        MiniBatchDispatcher(final BatchReadStore store, final ColumnarValueSchema schema, final int miniBatchSize) {
            m_batchReader = store.createRandomAccessReader();
            m_numBatches = store.numBatches();
            m_schema = schema;
            m_miniBatchSize = miniBatchSize;
        }

        boolean moreBatchesOnTheWay(final int consumedBatches) {
            return consumedBatches < m_dispatchedBatches.get() || m_running.get();
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < m_numBatches; i++) {//NOSONAR
                    ReadBatch batch = null;
                    try {
                        batch = m_batchReader.readRetained(i);
                        dispatchMiniBatches(batch);
                    } catch (IOException ex) {
                        // TODO better handling, maybe via a Future?
                        throw new IllegalArgumentException("Failed to read batch.", ex);
                    } finally {
                        if (batch != null) {
                            batch.release();
                        }
                    }
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                close();
            } finally {
                close();
                m_running.set(false);
            }
        }

        private void dispatchMiniBatches(final ReadBatch batch) throws InterruptedException {
            var batchSize = batch.length();
            for (int i = 0; i < batchSize; i += m_miniBatchSize) {//NOSONAR
                // we close it if we are interrupted while we wait for enqueueing it but once it is enqueued
                // it is no longer our responsibility to close it here
                @SuppressWarnings("resource")
                var miniBatch = new MiniReadBatch(m_schema, batch, i, i + m_miniBatchSize);
                try {
                    m_miniBatchQueue.put(miniBatch);
                    m_dispatchedBatches.incrementAndGet();
                } catch (InterruptedException ex) {
                    miniBatch.close();
                    Thread.currentThread().interrupt();
                    throw ex;
                }
            }
        }

        @Override
        public void close() {
            try {
                m_batchReader.close();
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to close the batch reader.");
            }
        }

    }

    private static final class MiniReadBatch implements RowTaskBatch<RowAccess> {

        static final MiniReadBatch NULL = new MiniReadBatch(null, null, 0, 0);

        private final ReadBatch m_batch;

        private final int m_startIdx;

        private final int m_endIdx;

        private final ColumnarValueSchema m_schema;

        MiniReadBatch(final ColumnarValueSchema schema, final ReadBatch batch, final int startIdx, final int endIdx) {
            if (batch != null) {
                batch.retain();
            }
            m_batch = batch;
            m_startIdx = startIdx;
            m_endIdx = endIdx;
            m_schema = schema;
        }

        @Override
        public void close() {
            m_batch.release();
        }

        @Override
        public ColumnTask createColumnTask(final int colIdx, final RowAccess writeAccess) {
            final var valueFactory = m_schema.getValueFactory(colIdx);
            final var data = m_batch.get(colIdx);
            if (colIdx == 0) {
                return new KeyCreator<>(writeAccess, valueFactory, data);
            } else {
                return new CellCreator<>(writeAccess, valueFactory, data);
            }
        }

        // TODO composition over inheritance
        private abstract class AbstractCreator<R extends ReadAccess> implements ColumnTask, ColumnDataIndex {

            protected final RowAccess m_access;

            protected final ReadValue m_readValue;

            private int m_idx;

            AbstractCreator(final RowAccess access, final ValueFactory<R, ?> valueFactory,
                final NullableReadData data) {
                var dataSpec = valueFactory.getSpec();
                var readAccess = ColumnarAccessFactoryMapper.createAccessFactory(dataSpec).createReadAccess(this);
                m_access = access;
                m_readValue = valueFactory.createReadValue((R)readAccess);
            }

            @Override
            public int size() {
                return m_endIdx - m_startIdx;
            }

            @Override
            public void performSubtask(final int subtask) {
                m_idx = m_startIdx + subtask;
                m_access.setCell(m_readValue.getDataCell());
            }

            @Override
            public int getIndex() {
                return m_idx;
            }

            protected abstract void setValue();

        }

        private final class CellCreator<R extends ReadAccess> extends AbstractCreator<R> {

            CellCreator(final RowAccess access, final ValueFactory<R, ?> valueFactory, final NullableReadData data) {
                super(access, valueFactory, data);
            }

            @Override
            protected void setValue() {
                m_access.setCell(m_readValue.getDataCell());
            }

        }

        private final class KeyCreator<R extends ReadAccess> extends AbstractCreator<R> {

            KeyCreator(final RowAccess access, final ValueFactory<R, ?> valueFactory, final NullableReadData data) {
                super(access, valueFactory, data);
            }

            @Override
            protected void setValue() {
                m_access.setKey(new RowKey(((RowKeyReadValue)m_readValue).getString()));
            }

        }

    }

    interface RowAccess {

        void setCell(final DataCell cell);

        void setKey(final RowKey key);
    }

    private static final class RowBatch implements Iterable<DataRow> {
        private final DataCell[][] m_rows;

        private final RowKey[] m_keys;

        private final int m_numRows;

        RowBatch(final RowKey[] keys, final DataCell[][] rows, final int numRows) {
            m_rows = rows;
            m_keys = keys;
            m_numRows = numRows;
        }

        @Override
        public Iterator<DataRow> iterator() {
            return new RowIter();
        }

        private class RowIter implements Iterator<DataRow> {

            private int m_idx = 0;

            @Override
            public boolean hasNext() {
                return m_idx < m_numRows;
            }

            @Override
            public DataRow next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                var idx = m_idx;
                m_idx++;
                return new RowBatchRow(idx);
            }

        }

        private final class RowBatchRow implements DataRow {

            private final int m_idx;

            RowBatchRow(final int idx) {
                m_idx = idx;
            }

            @Override
            public Iterator<DataCell> iterator() {
                return Iterators.forArray(m_rows[m_idx]);
            }

            @Override
            public int getNumCells() {
                return m_rows[m_idx].length;
            }

            @Override
            public RowKey getKey() {
                return m_keys[m_idx];
            }

            @Override
            public DataCell getCell(final int index) {
                return m_rows[m_idx][index];
            }

        }
    }

    private final class RowBatchBuilder implements GridWriter<RowAccess> {
        private final int m_numRows;

        private final int m_numCols;

        private DataCell[][] m_cells;

        private RowKey[] m_keys;

        private final AbstractFiller[] m_fillers;

        private final AtomicInteger m_finalizeBatchBarrier;

        RowBatchBuilder(final int numRows, final int numCols) {
            m_numCols = numCols;
            m_numRows = numRows;
            m_cells = new DataCell[m_numRows][m_numCols];
            m_keys = new RowKey[m_numRows];
            m_finalizeBatchBarrier = new AtomicInteger(m_numCols);
            m_fillers = Stream.concat(//
                Stream.of(new KeyFiller()), //
                IntStream.range(0, numCols)//
                    .mapToObj(ColumnFiller::new)//
            )//
                .toArray(AbstractFiller[]::new);
        }

        RowBatch build() {
            var cells = m_cells;
            var keys = m_keys;
            m_keys = new RowKey[m_numRows];
            m_cells = new DataCell[m_numRows][m_numCols];
            return new RowBatch(keys, cells, m_fillers[0].m_idx.get());
        }

        @Override
        public void close() throws IOException {
            m_keys = null;
            m_cells = null;
        }

        @Override
        public DataWriter<RowAccess> getDataWriter(final int colIdx) {
            return m_fillers[colIdx];
        }

        @Override
        public int numWriters() {
            return m_fillers.length;
        }

        @Override
        public void finishLastBatch() throws IOException {
            endBatch();
        }

        private void endBatch() {
            try {
                m_rowBatchQueue.put(build());
            } catch (InterruptedException ex) {
                // TODO perhaps that is just fine?
                throw new IllegalStateException("Interrupted while waiting for enqueuing the next batch.", ex);
            }
        }

        private void switchBatch() {
            endBatch();
            for (var filler : m_fillers) {
                filler.m_idx.set(0);
            }
        }

        private abstract class AbstractFiller implements DataWriter<RowAccess>, RowAccess {

            protected AtomicInteger m_idx = new AtomicInteger();

            AbstractFiller() {
            }

            @Override
            public boolean canWrite() {
                return m_idx.get() < m_numRows;
            }

            @Override
            public void advance() {
                if (m_idx.incrementAndGet() == m_numRows && m_finalizeBatchBarrier.decrementAndGet() == 0) {
                    switchBatch();
                }
            }

            @Override
            public RowAccess getAccess() {
                return this;
            }

        }

        private final class ColumnFiller extends AbstractFiller {

            private final int m_colIdx;

            ColumnFiller(final int colIdx) {
                m_colIdx = colIdx;
            }

            @Override
            public void setCell(final DataCell cell) {
                m_cells[m_idx.get()][m_colIdx] = cell;
            }

            @Override
            public void setKey(final RowKey key) {
                throw new IllegalStateException("Coding issue: Attempted to set the key on a ColumnFiller.");
            }
        }

        private final class KeyFiller extends AbstractFiller {

            @Override
            public void setCell(final DataCell cell) {
                throw new IllegalStateException("Coding issue: Attempted to set a cell on a KeyFiller.");
            }

            @Override
            public void setKey(final RowKey key) {
                m_keys[m_idx.get()] = key;
            }

        }

    }

}
