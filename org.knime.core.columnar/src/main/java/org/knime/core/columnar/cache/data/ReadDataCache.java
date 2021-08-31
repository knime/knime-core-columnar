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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.knime.core.columnar.cache.writable.SharedBatchWritableCache;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchWritable} and {@link RandomAccessBatchReadable} that holds and provides written {@link ReadData} in a
 * fixed-size {@link SharedBatchWritableCache LRU cache} in memory. It asynchronously flushes written data on to a
 * delegate. When any unflushed data is evicted from the cache, it blocks until that data has been flushed. On cache
 * miss (i.e., when any evicted, flushed data is read), it blocks until all data is {@link Flushable#flush() flushed}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ReadDataCache implements BatchWritable, RandomAccessBatchReadable, Flushable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDataCache.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for asynchronous write thread.";

    private final class ReadDataCacheWriter implements BatchWriter {

        private final Evictor<ColumnDataUniqueId, NullableReadData> m_evictor = (k, c) -> { // NOSONAR
            try {
                final CountDownLatch latch = m_cachedData.remove(k);
                if (latch != null) {
                    latch.await();
                }

            } catch (InterruptedException e) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
                // when interrupted here (e.g., because some other node that pushes data into the cache is cancelled), we
                // either lose the unflushed, to-be-evicted data, because it will be released once evicted from the cache
                // or we retain it and accept that we temporarily use up more off-heap memory than the cache allows
                c.retain();
                @SuppressWarnings("resource")
                ReadDataCache store = (ReadDataCache)k.getReadable();
                store.enqueueRunnable(c::release);
                // We can easily get into this state when cancelling a node that writes a lot of data. A warning should
                // therefore be sufficient here.
                LOGGER.warn("{} Unflushed data evicted from cache. "
                    + "Data will be retained and memory will be allocated until flushed.", ERROR_ON_INTERRUPT);
            }
        };

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

            batch.retain();

            handleDoneFuture();

            final CountDownLatch batchFlushed = new CountDownLatch(1);
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
                    batch.release();
                    batchFlushed.countDown();
                }
            });

            for (int i = 0; i < getSchema().numColumns(); i++) {
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(ReadDataCache.this, i, m_numBatches);
                m_cachedData.put(ccUID, batchFlushed);
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

    }

    private final class ReadDataCacheReader implements RandomAccessBatchReader {

        private final Evictor<ColumnDataUniqueId, NullableReadData> m_evictor = (k, c) -> m_cachedData.remove(k);

        private final ColumnSelection m_selection;

        ReadDataCacheReader(final ColumnSelection selection) {
            m_selection = selection;
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            return m_selection.createBatch(i -> { // NOSONAR
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(ReadDataCache.this, i, index);
                final CountDownLatch lock = m_cachedData.computeIfAbsent(ccUID, k -> new CountDownLatch(0));

                synchronized (lock) {
                    final NullableReadData cachedData = m_globalCache.getRetained(ccUID);
                    if (cachedData != null) {
                        return cachedData;
                    }

                    try {
                        waitForAndHandleFuture();
                    } catch (InterruptedException e) {
                        // Restore interrupted state...
                        Thread.currentThread().interrupt();
                        // when interrupted here (e.g., because the reading node is cancelled), we should not proceed
                        // this way, the cache stays in a consistent state
                        throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
                    }
                    try (RandomAccessBatchReader reader = m_reabableDelegate
                        .createRandomAccessReader(new FilteredColumnSelection(m_selection.numColumns(), i))) {
                        final NullableReadData data = reader.readRetained(index).get(i);
                        m_globalCache.put(ccUID, data, m_evictor);
                        return data;
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }
            });
        }

        @Override
        public void close() throws IOException {
            // no resources held
        }

    }

    private final ReadDataCacheWriter m_writer;

    private final RandomAccessBatchReadable m_reabableDelegate;

    private final ExecutorService m_executor;

    private final EvictingCache<ColumnDataUniqueId, NullableReadData> m_globalCache;

    private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

    private final Map<ColumnDataUniqueId, CountDownLatch> m_cachedData = new ConcurrentHashMap<>();

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    /**
     * @param delegate the delegate to which to write asynchronously and from which to read in case of a cache miss
     * @param cache the cache for storing data
     * @param executor the executor to which to submit asynchronous writes to the delegate
     */
    @SuppressWarnings("resource")
    public <D extends BatchWritable & RandomAccessBatchReadable> ReadDataCache(final D delegate,
        final SharedReadDataCache cache, final ExecutorService executor) {
        m_writer = new ReadDataCacheWriter(delegate.getWriter());
        m_reabableDelegate = delegate;
        m_globalCache = cache.getCache();
        m_executor = executor;
    }

    void enqueueRunnable(final Runnable r) {
        m_future = m_future.thenRunAsync(r, m_executor);
    }

    void waitForAndHandleFuture() throws InterruptedException {
        try {
            m_future.get();
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Failed to asynchronously write cached rows to file.", e);
        }
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
        return m_reabableDelegate.getSchema();
    }

    @Override
    public synchronized void close() throws IOException {
        if (!m_closed.getAndSet(true)) {

            waitForAllTasksToFinish();

            m_writer.close();

            releaseAllReferencedData();

            m_cachedData.clear();
            m_reabableDelegate.close();
        }
    }

    private void waitForAllTasksToFinish() throws IOException {
        // We use a {@link CountDownLatch} to _really_ wait for all tasks to finish because
        // flush() could be interrupted or pick up that the interrupt flag was set. Unfortunately,
        // the {@link CompletableFuture}s will still continue to be executed, hence we wait with
        // a latch that is counted down after everything else.
        final var closedLatch = new CountDownLatch(1);
        m_future = m_future.thenRunAsync(closedLatch::countDown, m_executor);
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while waiting for tasks to finish");
        }

        try {
            waitForAndHandleFuture();
        } catch (InterruptedException e) {
            throw new IOException("Close should not be interrupted!", e);
        }
    }

    private void releaseAllReferencedData() {
        // Drop all globally cached data referenced by this cache, because "release"-on-evict would
        // no longer work once the delegate is closed.
        for (final var ccUID : m_cachedData.keySet()) {
            final var lock = m_cachedData.get(ccUID);
            if (lock != null) {
                try {
                    lock.await();
                } catch (InterruptedException ex) {
                    LOGGER.error("Interrupted while waiting for cached data to be flushed");
                }
            }

            final var data = m_globalCache.removeRetained(ccUID);
            if (data != null) {
                data.release();
            }
        }
    }

}
