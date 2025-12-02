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
package org.knime.core.columnar.cache.data;

import java.io.Flushable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.data.SharedReadDataCache.ColumnDataKey;
import org.knime.core.columnar.filter.ColumnSelection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchWritable} and {@link RandomAccessBatchReadable} that holds and provides written {@link ReadData} in a
 * fixed-size {@link SharedReadDataCache LRU cache} in memory. It asynchronously flushes written data on to a delegate.
 * When any unflushed data is evicted from the cache, it blocks until that data has been flushed. On cache miss (i.e.,
 * when any evicted, flushed data is read), it blocks until all data is {@link Flushable#flush() flushed}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Tobias Pietzsch
 */
public final class ReadDataCache extends ReadDataReadCache implements BatchWritable, Flushable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDataCache.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for asynchronous write thread.";

    final class ReadDataCacheReader extends ReadDataReadCacheReader {
        ReadDataCacheReader(final ColumnSelection selection) {
            super(selection);
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            var writingBatch = m_currentlyWritingBatches.getRetained(index);
            return writingBatch != null ? writingBatch : super.readRetained(index);
        }
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new ReadDataCacheReader(selection);
    }

    private final class ReadDataCacheWriter implements BatchWriter {

        private final BatchWriter m_writerDelegate;

        private int m_numBatches;

        ReadDataCacheWriter(final BatchWriter delegate) {
            m_writerDelegate = delegate;
        }

        @Override
        public WriteBatch create(final int capacity) {
            return m_writerDelegate.create(capacity);
        }

        @Override
        public synchronized void write(final ReadBatch batch) throws IOException {

            final var batchId = m_numBatches;

            // NB: The following order makes sure that readers see the batch
            // consistently (or not at all):
            //
            //  1) Put the batch into m_currentlyWritingBatches.
            //  2) Put the batch's column datas into the global cache.
            //  3) Enqueue the write task (which will remove the batch from
            //     m_currentlyWritingBatches when it's done).
            //
            // Then, either
            //  1) The batch is absent from m_currentlyWritingBatches, the
            //     global cache, and the delegate reader. Or
            //  2) The full batch is in m_currentlyWritingBatches (this is where
            //     the reader looks first, so it doesn't matter that a few columns
            //     might be in the global cache already). Or
            //  3) All the batch's column datas have been written and are
            //     available from the delegate reader (and perhaps the global
            //     cache).

            m_currentlyWritingBatches.putRetained(batchId, batch);

            for (int i = 0; i < getSchema().numColumns(); i++) {
                final ColumnDataKey ccUID = new ColumnDataKey(m_ownerHandle, i, batchId);
                put(ccUID, batch.get(i));
            }

            m_futureQueue.handleDoneFuture();
            m_futureQueue.enqueueRunnable(() -> { // NOSONAR
                try {
                    if (!m_closed.get()) {
                        // No need to write to delegate if we're closed already.
                        // If the data was meant to be saved completely, flush should have been called before!
                        m_writerDelegate.write(batch);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(String.format("Failed to write batch %d.", batchId), e);
                } finally {
                    m_currentlyWritingBatches.remove(batchId);
                }
            });

            m_numBatches++;
        }

        private boolean m_writer_closed = false;

        @Override
        public synchronized void close() {
            if (!m_writer_closed) {
                m_writer_closed = true;
                m_futureQueue.handleDoneFuture();
                m_futureQueue.enqueueRunnable(() -> {
                    try {
                        m_writerDelegate.close();
                    } catch (IOException e) {
                        throw new IllegalStateException("Failed to close writer.", e);
                    }
                });
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return m_writerDelegate.initialNumBytesPerElement();
        }
    }

    private final ReadDataCacheWriter m_writer;

    private final FutureQueue m_futureQueue;

    private final ReadBatchRetainingMap m_currentlyWritingBatches = new ReadBatchRetainingMap();

    /**
     * @param writable the delegate to which to write asynchronously
     * @param readable the delegate from which to read in case of a cache miss
     * @param cache the cache for storing data
     * @param executor the executor to which to submit asynchronous writes to the delegate
     */
    public ReadDataCache(final BatchWritable writable, final RandomAccessBatchReadable readable,
                         final SharedReadDataCache cache, final ExecutorService executor) {

        super(readable, cache);

        m_writer = new ReadDataCacheWriter(writable.getWriter());
        m_futureQueue = new FutureQueue(executor);
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public void flush() throws IOException {
        try {
            m_futureQueue.waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            LOGGER.info(ERROR_ON_INTERRUPT);
        }
    }

    @Override
    void _close() throws IOException {

        waitForAllTasksToFinish();

        m_writer.close();
        try {
            // Wait for close to finish
            m_futureQueue.waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // The interrupt status should have been reset by some previous wait operation,
            // so if we get interrupted here, something has gone wrong and we throw an exception.
            throw new IOException("Close should not be interrupted!", e);
        }

        // This should be empty already, unless a write task threw an exception.
        m_currentlyWritingBatches.clear();

        super._close();
    }

    /**
     * Helper class for remembering batches that we are currently writing. Using {@link #getRetained(int)} we can be
     * sure that the batch won't get released before we are able to retain it.
     */
    private static final class ReadBatchRetainingMap {

        private final Map<Integer, ReadBatch> m_batches = new HashMap<>();

        public synchronized void putRetained(final int i, final ReadBatch batch) {
            batch.retain();
            m_batches.put(i, batch);
        }

        public synchronized void remove(final int i) {
            m_batches.remove(i).release();
        }

        public synchronized void clear() {
            m_batches.values().forEach(ReferencedData::release);
            m_batches.clear();
        }

        public synchronized ReadBatch getRetained(final int i) {
            var batch = m_batches.get(i);
            if (batch == null) {
                return null;
            }
            batch.retain();
            return batch;
        }
    }

    private void waitForAllTasksToFinish() throws IOException {
        // We use a {@link CountDownLatch} to _really_ wait for all tasks to finish because
        // flush() could be interrupted or pick up that the interrupt flag was set. Unfortunately,
        // the {@link CompletableFuture}s will still continue to be executed, hence we wait with
        // a latch that is counted down after everything else.
        final var closedLatch = new CountDownLatch(1);
        m_futureQueue.enqueueRunnable(closedLatch::countDown);
        m_futureQueue.enqueueExceptionHandler(ex -> closedLatch.countDown()); // also count down in case of a previous exception
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while waiting for tasks to finish");
        }

        try {
            // There should not be any tasks left after the previous block,
            // so this should return immediately. But we still invoke m_future.get()
            // to notice whether exceptions were thrown.
            m_futureQueue.waitForAndHandleFuture();
        } catch (InterruptedException e) {
            throw new IOException("Close should not be interrupted!", e);
        }
    }

    /**
     * Helper class for managing a chain of Futures
     */
    private static final class FutureQueue
    {
        private volatile CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

        private final ExecutorService m_executor;

        FutureQueue(final ExecutorService executor) {
            m_executor = executor;
        }

        synchronized void enqueueRunnable(final Runnable r) {
            m_future = m_future.thenRunAsync(r, m_executor);
        }

        /**
         * Add a runnable to the chain of futures that is executed if and only if any of the previous futures terminated
         * exceptionally.
         */
        synchronized void enqueueExceptionHandler(final Consumer<? super Throwable> handler) {
            m_future = m_future.exceptionallyAsync(throwable -> {
                handler.accept(throwable);
                return null; // Need to return null because a (void) return value is expected here
            }, m_executor);
        }

        void waitForAndHandleFuture() throws InterruptedException {
            try {
                m_future.get();
            } catch (final ExecutionException e) {
                throw wrap(e.getCause());
            }
        }

        void handleDoneFuture() {
            final CompletableFuture<Void> future = m_future;
            if (future.isDone()) {
                try {
                    try {
                        future.get();
                    } catch (final ExecutionException e) {
                        throw wrap(e.getCause());
                    }
                } catch (InterruptedException e) {
                    // Restore interrupted state
                    Thread.currentThread().interrupt();
                    // since we just checked whether the future is done, we likely never end up in this code block
                    throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
                }
            }
        }

        private static RuntimeException wrap(final Throwable t) {
            final String error;
            if (t.getMessage() != null) {
                error = t.getMessage();
            } else {
                error = "Failed to asynchronously write cached rows to file.";
            }
            return new IllegalStateException(error, t);
        }
    }
}
