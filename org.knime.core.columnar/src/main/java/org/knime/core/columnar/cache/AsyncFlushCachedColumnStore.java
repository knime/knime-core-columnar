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
package org.knime.core.columnar.cache;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.EvictingCache.Evictor;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.BatchWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.DelegatingColumnReadStore.DelegatingBatchReader;
import org.knime.core.columnar.store.DelegatingColumnStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ColumnStore column store} that stores {@link NullableWriteData data} in a fixed-size
 * {@link SmallColumnStoreCache LRU cache} in memory and asynchronously passes (flushes) data on to a delegate column
 * store. When any unflushed data is evicted from the cache, the store blocks until that data has been flushed. On cache
 * miss (i.e., when any evicted, flushed data is read), it blocks until the asynchronous write process has fully
 * terminated. The store allows concurrent reading via multiple {@link BatchReader readers} once the {@link BatchWriter
 * writer} has been closed.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class AsyncFlushCachedColumnStore extends DelegatingColumnStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFlushCachedColumnStore.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for asynchronous write thread.";

    private final class AsyncFlushCachedBatchWriter extends DelegatingBatchWriter {

        AsyncFlushCachedBatchWriter() {
            super(AsyncFlushCachedColumnStore.this);
        }

        @Override
        protected void writeInternal(final ReadBatch batch) throws IOException {

            batch.retain();

            handleDoneFuture();

            final CountDownLatch batchFlushed = new CountDownLatch(1);
            enqueueRunnable(() -> { // NOSONAR
                try {
                    if (!AsyncFlushCachedColumnStore.this.isClosed()) {
                        super.writeInternal(batch);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(String.format("Failed to write batch %d.", m_numChunks), e);
                } finally {
                    batch.release();
                    batchFlushed.countDown();
                }
            });

            m_maxDataCapacity = Math.max(m_maxDataCapacity, batch.length());
            for (int i = 0; i < getSchema().numColumns(); i++) {
                final ColumnDataUniqueId ccUID =
                    new ColumnDataUniqueId(AsyncFlushCachedColumnStore.this, i, m_numChunks);
                m_cachedData.put(ccUID, batchFlushed);
                m_globalCache.put(ccUID, batch.get(i), m_evictor);
            }
            m_numChunks++;
        }

        @Override
        protected void closeOnce() {
            handleDoneFuture();
            enqueueRunnable(() -> {
                try {
                    super.closeOnce();
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

    private final class AsyncFlushCachedBatchReader extends DelegatingBatchReader {

        private final BufferedBatchLoader m_batchLoader;

        AsyncFlushCachedBatchReader(final ColumnSelection selection) {
            super(AsyncFlushCachedColumnStore.this, selection);
            m_batchLoader = new BufferedBatchLoader();
        }

        @Override
        protected ReadBatch readRetainedInternal(final int index) throws IOException {
            return getSelection().createBatch(i -> { // NOSONAR
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(AsyncFlushCachedColumnStore.this, i, index);
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
                    try {
                        @SuppressWarnings("resource")
                        final NullableReadData data = m_batchLoader.loadBatch(initAndGetDelegate(), index).get(i);
                        data.retain();
                        m_globalCache.put(ccUID, data, m_evictor);
                        return data;
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }
            });
        }

        @Override
        public int numBatches() {
            return m_numChunks;
        }

        @Override
        public int maxLength() {
            return m_maxDataCapacity;
        }

        @Override
        protected void closeOnce() throws IOException {
            m_batchLoader.close();
            super.closeOnce();
        }

    }

    private final EvictingCache<ColumnDataUniqueId, NullableReadData> m_globalCache;

    private Map<ColumnDataUniqueId, CountDownLatch> m_cachedData = new ConcurrentHashMap<>();

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
            AsyncFlushCachedColumnStore store = (AsyncFlushCachedColumnStore)k.getStore();
            store.enqueueRunnable(c::release);
            // We can easily get into this state when cancelling a node that writes a lot of data. A warning should
            // therefore be sufficient here.
            LOGGER.warn("{} Unflushed data evicted from cache. "
                + "Data will be retained and memory will be allocated until flushed.", ERROR_ON_INTERRUPT);
        }
    };

    private final ExecutorService m_executor;

    private int m_numChunks = 0;

    private int m_maxDataCapacity = 0;

    private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

    /**
     * @param delegate the delegate to which to write asynchronously
     * @param cache the cache for storing data
     * @param executor the executor to which to submit asynchronous writes to the delegate
     */
    public AsyncFlushCachedColumnStore(final ColumnStore delegate, final CachedColumnStoreCache cache,
        final ExecutorService executor) {
        super(delegate);
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
    protected BatchWriter createWriterInternal() {
        return new AsyncFlushCachedBatchWriter();
    }

    @Override
    protected void saveInternal(final File f) throws IOException {
        try {
            waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            LOGGER.info(ERROR_ON_INTERRUPT);
            return;
        }
        super.saveInternal(f);
    }

    @Override
    protected BatchReader createReaderInternal(final ColumnSelection selection) {
        return new AsyncFlushCachedBatchReader(selection);
    }

    @Override
    protected void closeOnce() throws IOException {
        try {
            waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            LOGGER.info(ERROR_ON_INTERRUPT);
        }

        for (final ColumnDataUniqueId id : m_cachedData.keySet()) {
            final NullableReadData removed = m_globalCache.removeRetained(id);
            if (removed != null) {
                removed.release();
            }
        }
        m_cachedData.clear();
        m_cachedData = null;
        super.closeOnce();
    }

}
