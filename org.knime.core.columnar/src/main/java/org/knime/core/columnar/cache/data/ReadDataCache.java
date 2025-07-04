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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.EvictingCache;
import org.knime.core.columnar.cache.EvictingCache.Evictor;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchWritable} and {@link RandomAccessBatchReadable} that holds and provides written {@link ReadData} in a
 * fixed-size {@link SharedReadDataCache LRU cache} in memory. It asynchronously flushes written data on to a delegate.
 * When any unflushed data is evicted from the cache, it blocks until that data has been flushed. On cache miss (i.e.,
 * when any evicted, flushed data is read), it blocks until all data is {@link Flushable#flush() flushed}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ReadDataCache implements BatchWritable, RandomAccessBatchReadable, Flushable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDataCache.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for asynchronous write thread.";

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
            m_currentlyWritingBatches.putRetained(batchId, batch);

            handleDoneFuture();

            enqueueRunnable(() -> { // NOSONAR
                try {
                    if (!m_closed.get()) {
                        // No need to write to delegate if we're closed already.
                        // If the data was meant to be saved completely, flush should have been called before!
                        m_writerDelegate.write(batch);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(String.format("Failed to write batch %d.", m_numBatches), e);
                } finally {
                    m_currentlyWritingBatches.remove(batchId);
                }
            });

            for (int i = 0; i < getSchema().numColumns(); i++) {
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(ReadDataCache.this, i, m_numBatches);
                m_cachedDataIds.put(ccUID, new Object());
                m_globalCache.put(ccUID, batch.get(i), m_evictor);
            }
            m_numBatches++;
        }

        @Override
        public synchronized void close() {
            handleDoneFuture();
            enqueueRunnable(() -> {
                try {
                    m_writerDelegate.close();
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to close writer.", e);
                }
            });
        }

        private void handleDoneFuture() {
            if (m_future.isDone()) {
                try {
                    waitForAndHandleFuture();
                } catch (InterruptedException e) {
                    // Restore interrupted state
                    Thread.currentThread().interrupt();
                    // since we just checked whether the future is done, we likely never end up in this code block
                    throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
                }
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return m_writerDelegate.initialNumBytesPerElement();
        }

    }

    private final class ReadDataCacheReader implements RandomAccessBatchReader {

        private final ColumnSelection m_selection;

        private final int[] m_selectedColumns;

        ReadDataCacheReader(final ColumnSelection selection) {
            m_selection = selection;
            m_selectedColumns = getSelectedColumns(selection);
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            var writingBatch = m_currentlyWritingBatches.getRetained(index);
            if (writingBatch != null) {
                return writingBatch;
            }

            final int numColumns = m_selection.numColumns();
            final NullableReadData[] datas = new NullableReadData[numColumns];
            int[] missingCols = new int[m_selectedColumns.length];
            int numMissing = 0;
            for (int i : m_selectedColumns) {
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(ReadDataCache.this, i, index);
                final NullableReadData cachedData = m_globalCache.getRetained(ccUID);
                if (cachedData != null) {
                    datas[i] = cachedData;
                } else {
                    missingCols[numMissing++] = i;
                }
            }
            if (numMissing > 0) {
                missingCols = Arrays.copyOf(missingCols, numMissing);

                try {
                    waitForAndHandleFuture();
                } catch (InterruptedException e) {
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                    // when interrupted here (e.g., because the reading node is cancelled), we should not proceed
                    // this way, the cache stays in a consistent state
                    throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
                }

                // we use the first column's id as a proxy for locking.
                final ColumnDataUniqueId lockUID = new ColumnDataUniqueId(ReadDataCache.this, 0, index);
                final Object lock = m_cachedDataIds.computeIfAbsent(lockUID, k -> new Object());
                synchronized (lock) {
                    try (RandomAccessBatchReader reader = m_readableDelegate
                        .createRandomAccessReader(new FilteredColumnSelection(numColumns, missingCols))) {
                        final ReadBatch batch = reader.readRetained(index);
                        for (int i : missingCols) {
                            final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(ReadDataCache.this, i, index);
                            final NullableReadData data = batch.get(i);
                            final NullableReadData cachedData = m_globalCache.getRetained(ccUID);
                            if (cachedData != null) {
                                data.release();
                                datas[i] = cachedData;
                            } else {
                                m_cachedDataIds.computeIfAbsent(ccUID, k -> new Object());
                                m_globalCache.put(ccUID, data, m_evictor);
                                datas[i] = data;
                            }
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }
            }

            return m_selection.createBatch(i -> datas[i]);
        }

        @Override
        public void close() throws IOException {
            // no resources held
        }

    }

    private final ReadDataCacheWriter m_writer;

    private final RandomAccessBatchReadable m_readableDelegate;

    private final ExecutorService m_executor;

    private final EvictingCache<ColumnDataUniqueId, NullableReadData> m_globalCache;

    private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

    private final Map<ColumnDataUniqueId, Object> m_cachedDataIds = new ConcurrentHashMap<>();

    private final Evictor<ColumnDataUniqueId, NullableReadData> m_evictor = (k, c) -> m_cachedDataIds.remove(k);

    private final ReadBatchRetainingMap m_currentlyWritingBatches = new ReadBatchRetainingMap();

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    /**
     * @param writable the delegate to which to write asynchronously
     * @param readable the delegate from which to read in case of a cache miss
     * @param cache the cache for storing data
     * @param executor the executor to which to submit asynchronous writes to the delegate
     */
    @SuppressWarnings("resource")
    public ReadDataCache(final BatchWritable writable, final RandomAccessBatchReadable readable,
        final SharedReadDataCache cache, final ExecutorService executor) {

        m_writer = new ReadDataCacheWriter(writable.getWriter());
        m_readableDelegate = readable;
        m_globalCache = cache.getCache();
        m_executor = executor;
    }

    synchronized void enqueueRunnable(final Runnable r) {
        m_future = m_future.thenRunAsync(r, m_executor);
    }

    /**
     * Add a runnable to the chain of futures that is executed if and only if any of the previous futures terminated
     * exceptionally.
     */
    synchronized void enqueueExceptionHandler(final Consumer<Throwable> handler) {
        m_future = m_future.exceptionallyAsync((final Throwable exception) -> {
            handler.accept(exception);
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

    private static RuntimeException wrap(final Throwable t) {
        final String error;
        if (t.getMessage() != null) {
            error = t.getMessage();
        } else {
            error = "Failed to asynchronously write cached rows to file.";
        }
        return new IllegalStateException(error, t);
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public void flush() throws IOException {
        try {
            waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            LOGGER.info(ERROR_ON_INTERRUPT);
        }
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new ReadDataCacheReader(selection);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_readableDelegate.getSchema();
    }

    @Override
    public synchronized void close() throws IOException {
        if (!m_closed.getAndSet(true)) {

            waitForAllTasksToFinish();

            m_writer.close();
            try {
                // Wait for close to finish
                waitForAndHandleFuture();
            } catch (InterruptedException e) {
                // The interrupt status should have been reset by some previous wait operation,
                // so if we get interrupted here, something has gone wrong and we throw an exception.
                throw new IOException("Close should not be interrupted!", e);
            }

            releaseAllReferencedData();

            m_cachedDataIds.clear();
            m_readableDelegate.close();
        }
    }

    private void waitForAllTasksToFinish() throws IOException {
        // We use a {@link CountDownLatch} to _really_ wait for all tasks to finish because
        // flush() could be interrupted or pick up that the interrupt flag was set. Unfortunately,
        // the {@link CompletableFuture}s will still continue to be executed, hence we wait with
        // a latch that is counted down after everything else.
        final var closedLatch = new CountDownLatch(1);
        enqueueRunnable(closedLatch::countDown);
        enqueueExceptionHandler(ex -> closedLatch.countDown()); // also count down in case of a previous exception
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while waiting for tasks to finish");
        }

        try {
            // There should not be any tasks left after the previous block,
            // so this should return immediately. But we still invoke m_future.get()
            // to notice whether exceptions were thrown.
            waitForAndHandleFuture();
        } catch (InterruptedException e) {
            throw new IOException("Close should not be interrupted!", e);
        }
    }

    private void releaseAllReferencedData() {
        // Drop all globally cached data referenced by this cache
        for (final var entry : m_cachedDataIds.entrySet()) {
            final var ccUID = entry.getKey();
            final var lock = entry.getValue();

            synchronized (lock) {
                m_globalCache.remove(ccUID);
            }
        }
    }

    static int[] getSelectedColumns(final ColumnSelection s) {
        final int numColumns = s.numColumns();
        final int[] selected = new int[numColumns];
        int j = 0;
        for (int i = 0; i < numColumns; i++) {
            if (s.isSelected(i)) {
                selected[j++] = i;
            }
        }
        return j == numColumns ? selected : Arrays.copyOf(selected, j);
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

        public synchronized ReadBatch getRetained(final int i) {
            var batch = m_batches.get(i);
            if (batch == null) {
                return null;
            }
            batch.retain();
            return batch;
        }
    }

    @Override
    public long[] getBatchBoundaries() {
        return m_readableDelegate.getBatchBoundaries();
    }
}
