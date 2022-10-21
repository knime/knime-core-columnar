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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarReadAccess;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.parallel.exec.MultiBatchWriteTask;
import org.knime.core.columnar.parallel.exec.RowTaskBatch;
import org.knime.core.columnar.parallel.exec.WriteTaskExecutor;
import org.knime.core.columnar.parallel.write.AsyncBatchWriter;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
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

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4, new ThreadFactory() {
        private final AtomicInteger m_threadNum = new AtomicInteger(0);

        @Override
        public Thread newThread(final Runnable r) {
            return new Thread(r, "KNIME-Columnar-Write-Task-Executor-" + m_threadNum.getAndIncrement());
        }
    });

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
                var writeExecutor = new WriteTaskExecutor(asyncWriter, EXECUTOR, MultiBatchWriteTask.NULL);
                var writeDispatcher = new WriteDispatcher(schema, 10000, writeExecutor)//
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

        private final MultiBatchWriteTask.Builder m_taskBuilder;

        private final int m_taskSize;

        private final Consumer<RowTaskBatch> m_taskConsumer;

        private int m_taskCounter;

        WriteDispatcher(final ColumnarSchema schema, final int taskSize, final Consumer<RowTaskBatch> taskConsumer) {
            m_taskSize = taskSize;
            m_taskBuilder = MultiBatchWriteTask.builder(schema, taskSize);
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
