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
 *   Oct 8, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarReadAccess;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.data.columnar.DataTransfers.DataTransfer;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class BatchStoreFilterer {

    interface ReadAccessRowFilterFactory {
        interface ReadAccessRowFilter {
            boolean include(long index);
        }

        ReadAccessRowFilter createFilter(final ReadAccessRow access);
    }

    static long filter(final BatchReadStore source, final ReadAccessRowFilterFactory filterFactory,
        final BatchStore sink, final ExecutionMonitor monitor) throws IOException {
        var readIdx = new BatchIndex();
        final var readAccessRow = new FilterReadAccessRow(source.getSchema(), readIdx);
        final var filter = filterFactory.createFilter(readAccessRow);
        var size = 0L;
        var schema = sink.getSchema();
        double numBatches = source.numBatches();
        try (//
                var reader = source.createRandomAccessReader();
                var asyncWriter = new AsyncBatchWriter(sink, source.batchLength());
                var writeExecutor = new WriteTaskExecutor(//
                    IntStream.range(0, schema.numColumns())//
                        .mapToObj(asyncWriter::getDataWriter)//
                        .collect(Collectors.toList())//
                ); //
                var writeDispatcher = new WriteDispatcher(1000, writeExecutor)//
        ) {
            for (int b = 0; b < source.numBatches(); b++) {//NOSONAR
                var readBatch = reader.readRetained(b);
                readAccessRow.nextBatch(readBatch);
                for (readIdx.m_idx = 0; readIdx.m_idx < readBatch.length(); readIdx.m_idx++) {
                    if (filter.include(b + readIdx.m_idx)) {
                        size++;
                        writeDispatcher.accept(readBatch, readIdx.m_idx);
                    }
                }
                readBatch.release();
                monitor.setProgress((b + 1) / numBatches);
            }

            // TODO compose these 3 into a higher-level abstraction
            try {
                writeDispatcher.dispatchCurrentTask();
                writeExecutor.await();
                asyncWriter.finishLastBatch();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Writing was interrupted.", ex);
            }

            return size;
        }
    }

    private static final class WriteDispatcher implements ObjIntConsumer<ReadBatch>, AutoCloseable {

        private final RowWriteTask.Builder m_taskBuilder;

        private final int m_taskSize;

        private final Consumer<RowWriteTask> m_taskConsumer;

        private int m_taskCounter;

        WriteDispatcher(final int taskSize, final Consumer<RowWriteTask> taskConsumer) {
            m_taskSize = taskSize;
            m_taskBuilder = new RowWriteTask.Builder(taskSize);
            m_taskConsumer = taskConsumer;
        }

        @Override
        public void accept(final ReadBatch t, final int value) {
            m_taskBuilder.addPosition(value, t);
            m_taskCounter++;
            if (m_taskCounter == m_taskSize) {
                dispatchCurrentTask();
            }
        }

        void dispatchCurrentTask() {
            m_taskConsumer.accept(m_taskBuilder.build());
            m_taskCounter = 0;
        }

        @Override
        public void close() {
            m_taskBuilder.close();
        }

    }

    private static final class WriteTaskExecutor implements Consumer<RowWriteTask>, AutoCloseable {

        private static final ExecutorService EXECUTOR =
            Executors.newCachedThreadPool(r -> new Thread(r, "KNIME-Columnar-Write-Task-Executor-"));

        // might make this blocking and only allow a limited number of pending tasks
        private final BlockingQueue<RowWriteTask> m_tasks = new LinkedBlockingQueue<>(3);

        private final List<? extends ObjIntConsumer<NullableReadData>> m_columnWriters;

        private Future<?> m_currentTaskFuture;

        private final AtomicBoolean m_open = new AtomicBoolean(true);

        private final AtomicBoolean m_waitingForFinish = new AtomicBoolean(false);

        WriteTaskExecutor(final List<? extends ObjIntConsumer<NullableReadData>> columnWriters) {
            m_columnWriters = columnWriters;
            m_currentTaskFuture = EXECUTOR.submit(this::scheduleColumnTasks);
        }

        private void scheduleColumnTasks() {
            // null is the poison pill
            while (m_open.get()) {
                try (var task = m_tasks.take()) {
                    if (task == RowWriteTask.NULL) {
                        // poison pill
                        return;
                    }
                    var columnTaskFutures = new ArrayList<Future<?>>(m_columnWriters.size());
                    for (int c = 0; c < m_columnWriters.size(); c++) { //NOSONAR
                        var columnTask = new ColumnWriteTask(task, c, m_columnWriters.get(c));
                        columnTaskFutures.add(EXECUTOR.submit(columnTask));
                    }

                    for (var future : columnTaskFutures) {
                        // TODO should probably cancel all futures in this case
                        try {
                            future.get();
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(
                                "Interrupted while waiting for the completion of ColumnWriteTasks.", ex);
                        } catch (ExecutionException ex) { //NOSONAR just a wrapper, we report the actual cause
                            throw new IllegalStateException("A ColumnWriteTask failed.", ex.getCause());
                        }
                    }
                } catch (InterruptedException interruptWhileWaitingForTask) {
                    // TODO maybe that's just fine?
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for tasks.",
                        interruptWhileWaitingForTask);
                }
            }

        }

        @Override
        public void accept(final RowWriteTask t) {
            if (m_waitingForFinish.get()) {
                throw new IllegalStateException("Waiting for the queue to be finished. No more tasks can be added.");
            }
            try {
                m_tasks.put(t);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Writing has been interrupted.", ex);
            }
        }

        void await() throws InterruptedException {
            m_waitingForFinish.set(true);
            // insert the poison pill
            m_tasks.put(RowWriteTask.NULL);
            try {
                m_currentTaskFuture.get();
            } catch (ExecutionException ex) {//NOSONAR just a wrapper
                throw new IllegalStateException("Asynchronous writing failed.", ex.getCause());
            }
        }

        @Override
        public void close() {
            if (m_open.getAndSet(false)) {
                m_tasks.forEach(RowWriteTask::close);
                m_currentTaskFuture.cancel(true);
            }
        }

    }

    private static final class AsyncBatchWriter implements Closeable {

        private final BatchWriter m_writer;

        private WriteBatch m_currentBatch;

        private final CyclicBarrier m_finalizeBatchBarrier;

        private final int m_batchLength;

        private final AsyncDataWriter[] m_columnWriters;

        AsyncBatchWriter(final BatchStore batchStore, final int batchLength) {
            m_batchLength = batchLength;
            m_writer = batchStore.getWriter();
            final var schema = batchStore.getSchema();
            m_finalizeBatchBarrier = new CyclicBarrier(schema.numColumns(), this::switchBatch);
            m_columnWriters = schema.specStream()//
                .map(DataTransfers::createTransfer)//
                .map(AsyncDataWriter::new)//
                .toArray(AsyncDataWriter[]::new);
            startBatch();
        }

        AsyncDataWriter getDataWriter(final int colIdx) {
            return m_columnWriters[colIdx];
        }

        private void switchBatch() {
            finishBatch(m_batchLength);
            startBatch();
        }

        private void startBatch() {
            m_currentBatch = m_writer.create(m_batchLength);
            resetColumnWriters();
        }

        private void finishBatch(final int batchLength) {
            assert allColumnWritersHaveExpectedIndex(batchLength) : "The column writers are out of sync.";
            var finishedBatch = m_currentBatch.close(batchLength);
            try {
                m_writer.write(finishedBatch);
            } catch (IOException ex) {
                // TODO we probably need a more elaborate mechanism to properly work in the concurrent setting
                throw new IllegalStateException("Failed to write batch.", ex);
            }
            finishedBatch.release();
        }

        private void finishLastBatch() throws IOException {
            var currentBatchLength = getCurrentBatchLength();
            if (currentBatchLength > 0) {
                finishBatch(currentBatchLength);
            }
            close();
        }

        private int getCurrentBatchLength() {
            return m_columnWriters[0].currentIdx();
        }

        private boolean allColumnWritersHaveExpectedIndex(final int expected) {
            return Stream.of(m_columnWriters).mapToInt(AsyncDataWriter::currentIdx).allMatch(i -> i == expected);
        }

        private void resetColumnWriters() {
            for (int i = 0; i < m_currentBatch.numData(); i++) {
                m_columnWriters[i].reset(m_currentBatch.get(i));
            }
        }

        @Override
        public void close() throws IOException {
            m_finalizeBatchBarrier.reset();
            m_currentBatch = null;
            m_writer.close();
        }

        private final class AsyncDataWriter implements ObjIntConsumer<NullableReadData> {

            private final DataTransfer m_transfer;

            private NullableWriteData m_writeData;

            private int m_idx = 0;

            int currentIdx() {
                return m_idx;
            }

            AsyncDataWriter(final DataTransfer transfer) {
                m_transfer = transfer;
            }

            void reset(final NullableWriteData writeData) {
                m_writeData = writeData;
                m_idx = 0;
            }

            @Override
            public void accept(final NullableReadData readData, final int idx) {
                m_transfer.transfer(readData, idx, m_writeData, m_idx);
                m_idx++;
                if (m_idx == m_batchLength) {
                    try {
                        m_finalizeBatchBarrier.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while waiting for next batch.", ex);
                    } catch (BrokenBarrierException ex) {
                        throw new IllegalStateException("The barrier for the next batch has been broken.", ex);
                    }
                }

            }
        }

    }

    private static final class ColumnWriteTask implements Runnable {

        private final int m_colIdx;

        private final RowWriteTask m_task;

        private final ObjIntConsumer<NullableReadData> m_writer;

        ColumnWriteTask(final RowWriteTask task, final int colIdx, final ObjIntConsumer<NullableReadData> writer) {
            m_task = task;
            m_colIdx = colIdx;
            m_writer = writer;
        }

        @Override
        public void run() {
            for (int i = 0; i < m_task.size(); i++) { //NOSONAR
                var readData = m_task.getBatch(i).get(m_colIdx);
                m_writer.accept(readData, m_task.getBatchIndex(i));
            }
        }

    }

    private static final class RowWriteTask implements AutoCloseable {

        static final RowWriteTask NULL = new RowWriteTask(new ReadBatch[0], new int[0]);

        private final int[] m_indices;

        private final ReadBatch[] m_batches;

        private RowWriteTask(final ReadBatch[] batches, final int[] indices) {
            m_indices = indices;
            m_batches = batches;
        }

        int getBatchIndex(final int idx) {
            return m_indices[idx];
        }

        ReadBatch getBatch(final int idx) {
            return m_batches[idx];
        }

        int size() {
            return m_indices.length;
        }

        @Override
        public void close() {
            for (var batch : m_batches) {
                batch.release();
            }
        }

        private static final class Builder implements AutoCloseable {
            private final int[] m_indices;

            private final ReadBatch[] m_batches;

            private int m_currentIndex = 0;

            private Builder(final int length) {
                m_indices = new int[length];
                m_batches = new ReadBatch[length];
            }

            Builder addPosition(final int index, final ReadBatch batch) {
                batch.retain();
                m_indices[m_currentIndex] = index;
                m_batches[m_currentIndex] = batch;
                m_currentIndex++;
                return this;
            }

            @Override
            public void close() {
                IntStream.range(0, m_currentIndex).forEach(i -> m_batches[i].release());
            }

            RowWriteTask build() {
                var indices = Arrays.copyOf(m_indices, m_currentIndex);
                var batches = Arrays.copyOf(m_batches, m_currentIndex);
                m_currentIndex = 0;
                return new RowWriteTask(batches, indices);
            }
        }
    }

    // TODO find a better value
    private static final int BATCH_LENGTH = 32000;

    static long filter(final RowAccessible source, final ReadAccessRowFilterFactory filterFactory,
        final BatchStore sink, final ExecutionMonitor monitor) throws IOException {
        try (var cursor = source.createCursor(); var filterSink = new FilterSink(sink, BATCH_LENGTH)) {
            var access = cursor.access();
            double numRows = source.size();
            var filter = filterFactory.createFilter(access);
            var size = 0L;
            for (long i = 0; cursor.forward(); i++) {
                if (filter.include(i)) {
                    size++;
                    filterSink.accept(access);
                }
                monitor.setProgress((i + 1) / numRows);
            }
            filterSink.writeLastBatch();
            return size;
        }
    }

    private static final class FilterSink implements Closeable, ColumnDataIndex {

        private final int m_batchSize;

        private final BatchWriter m_writer;

        private final FilterWriteAccessRow m_writeAccess;

        private int m_idx = 0;

        private WriteBatch m_writeBatch;

        FilterSink(final BatchStore writeStore, final int batchSize) {
            m_writer = writeStore.getWriter();
            m_idx = 0;
            m_writeAccess = new FilterWriteAccessRow(writeStore.getSchema(), this);
            m_batchSize = batchSize;
            m_writeBatch = m_writer.create(batchSize);
        }

        void accept(final ReadAccessRow row) throws IOException {
            m_writeAccess.setFrom(row);
            m_idx++;
            if (m_idx >= BATCH_LENGTH) {
                writeCurrentBatch();
                startNewBatch();
            }
        }

        private void startNewBatch() {
            m_writeBatch = m_writer.create(m_batchSize);
            m_writeAccess.nextBatch(m_writeBatch);
            m_idx = 0;
        }

        private void writeCurrentBatch() throws IOException {
            final var finishedBatch = m_writeBatch.close(m_idx);
            m_writer.write(finishedBatch);
            finishedBatch.release();
        }

        void writeLastBatch() throws IOException {
            if (m_idx > 0) {
                writeCurrentBatch();
            }
            close();
        }

        @Override
        public int getIndex() {
            return m_idx;
        }

        @Override
        public void close() throws IOException {
            m_writer.close();
        }

    }

    private static final class BatchIndex implements ColumnDataIndex {
        int m_idx = 0;

        @Override
        public int getIndex() {
            return m_idx;
        }

    }

    private static final class FilterWriteAccessRow implements WriteAccessRow {

        private final ColumnarWriteAccess[] m_accesses;

        FilterWriteAccessRow(final ColumnarSchema schema, final ColumnDataIndex index) {
            m_accesses = schema.specStream()//
                .map(ColumnarAccessFactoryMapper::createAccessFactory)//
                .map(f -> f.createWriteAccess(index))//
                .toArray(ColumnarWriteAccess[]::new);
        }

        void nextBatch(final WriteBatch batch) {
            for (int i = 0; i < m_accesses.length; i++) {
                m_accesses[i].setData(batch.get(i));
            }
        }

        @Override
        public int size() {
            return m_accesses.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends WriteAccess> A getWriteAccess(final int index) {
            return (A)m_accesses[index];
        }

        @Override
        public void setFrom(final ReadAccessRow row) {
            for (int i = 0; i < m_accesses.length; i++) {//NOSONAR
                m_accesses[i].setFrom(row.getAccess(i));
            }
        }

    }

    private static final class FilterReadAccessRow implements ReadAccessRow {

        private final ColumnarReadAccess[] m_accesses;

        FilterReadAccessRow(final ColumnarSchema schema, final ColumnDataIndex index) {
            m_accesses = schema.specStream()//
                .map(ColumnarAccessFactoryMapper::createAccessFactory)//
                .map(f -> f.createReadAccess(index)).toArray(ColumnarReadAccess[]::new);
        }

        void nextBatch(final ReadBatch batch) {
            for (int i = 0; i < batch.numData(); i++) {
                m_accesses[i].setData(batch.get(i));
            }
        }

        @Override
        public int size() {
            return m_accesses.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            return (A)m_accesses[index];
        }

    }
}
