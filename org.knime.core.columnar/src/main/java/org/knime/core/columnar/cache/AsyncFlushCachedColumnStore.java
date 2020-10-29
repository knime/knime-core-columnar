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
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.LoadingEvictingCache.Evictor;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 * A {@link ColumnStore column store} that stores {@link ColumnWriteData data} in a fixed-size
 * {@link SmallColumnStoreCache LRU cache} in memory and asynchronously passes (flushes) data on to a delegate column
 * store. When any unflushed data is evicted from the cache, the store blocks until that data has been flushed. On cache
 * miss (i.e., when any evicted, flushed data is read), it blocks until the asynchronous write process has fully
 * terminated. The store allows concurrent reading via multiple {@link ColumnDataReader readers} once the
 * {@link ColumnDataWriter writer} has been closed.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class AsyncFlushCachedColumnStore implements ColumnStore {

    private static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

    private static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for asynchronous write thread.";

    private static final CountDownLatch DUMMY = new CountDownLatch(0);

    private class AsyncFlushCachedColumnStoreFactory implements ColumnDataFactory {

        private final ColumnDataFactory m_delegateFactory;

        AsyncFlushCachedColumnStoreFactory() {
            m_delegateFactory = m_delegate.getFactory();
        }

        @Override
        public WriteBatch create() {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            return m_delegateFactory.create();
        }

    }

    private final class AsyncFlushCachedColumnStoreWriter implements ColumnDataWriter {

        private final ColumnDataWriter m_delegateWriter;

        AsyncFlushCachedColumnStoreWriter() {
            m_delegateWriter = m_delegate.getWriter();
        }

        @Override
        public void write(final ReadBatch batch) throws IOException {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            batch.retain();

            handleDoneFuture();

            final CountDownLatch batchFlushed = new CountDownLatch(1);
            enqueueRunnable(() -> {
                try {
                    if (!m_storeClosed) {
                        m_delegateWriter.write(batch);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(String.format("Failed to write batch %d.", m_numChunks), e);
                } finally {
                    batch.release();
                    batchFlushed.countDown();
                }
            });

            m_maxDataCapacity = Math.max(m_maxDataCapacity, batch.length());
            for (int i = 0; i < getSchema().getNumColumns(); i++) {
                final ColumnDataUniqueId ccUID =
                    new ColumnDataUniqueId(AsyncFlushCachedColumnStore.this, i, m_numChunks);
                m_cachedData.put(ccUID, batchFlushed);
                m_globalCache.put(ccUID, batch.get(i), m_evictor);
            }
            m_numChunks++;
        }

        @Override
        public void close() {
            m_writerClosed = true;

            handleDoneFuture();

            enqueueRunnable(() -> {
                try {
                    m_delegateWriter.close();
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

    private final class AsyncFlushCachedColumnStoreReader implements ColumnDataReader {

        private final ColumnSelection m_selection;

        private final BufferedBatchLoader m_batchLoader;

        // lazily initialized
        private ColumnDataReader m_delegateReader;

        private boolean m_readerClosed;

        AsyncFlushCachedColumnStoreReader(final ColumnSelection selection) {
            m_selection = selection;
            m_batchLoader = new BufferedBatchLoader();
        }

        @Override
        public ReadBatch readRetained(final int chunkIndex) {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            return ColumnSelection.createBatch(m_selection, i -> {
                final ColumnDataUniqueId ccUID =
                    new ColumnDataUniqueId(AsyncFlushCachedColumnStore.this, i, chunkIndex);
                return m_globalCache.getRetained(ccUID, () -> {
                    try {
                        waitForAndHandleFuture();
                    } catch (InterruptedException e) {
                        // Restore interrupted state...
                        Thread.currentThread().interrupt();
                        // when interrupted here (e.g., because the reading node is cancelled), we should not proceed
                        // this way, the cache stays in a consistent state
                        throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
                    }
                    if (m_delegateReader == null) {
                        m_delegateReader = m_delegate.createReader(m_selection);
                    }
                    m_cachedData.put(ccUID, DUMMY);
                    try {
                        final ColumnReadData data = m_batchLoader.loadBatch(m_delegateReader, chunkIndex).get(i);
                        data.retain();
                        return data;
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }, m_evictor);
            });
        }

        @Override
        public int getNumBatches() {
            return m_numChunks;
        }

        @Override
        public int getMaxLength() {
            return m_maxDataCapacity;
        }

        @Override
        public void close() throws IOException {
            m_readerClosed = true;
            m_batchLoader.close();
            if (m_delegateReader != null) {
                m_delegateReader.close();
            }
        }

    }

    private final ColumnStore m_delegate;

    private final ColumnDataFactory m_factory;

    private final AsyncFlushCachedColumnStoreWriter m_writer;

    private final LoadingEvictingCache<ColumnDataUniqueId, ColumnReadData> m_globalCache;

    private final Map<ColumnDataUniqueId, CountDownLatch> m_cachedData = new ConcurrentHashMap<>();

    private final Evictor<ColumnDataUniqueId, ColumnReadData> m_evictor = (k, c) -> {
        try {
            m_cachedData.remove(k).await();
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
            System.err.println(String.format("%s "
                    + "Unflushed data evicted from cache. "
                    + "Data will be retained and memory will be allocated until flushed.", ERROR_ON_INTERRUPT));
        }
    };

    private final ExecutorService m_executor;

    private int m_numChunks = 0;

    private int m_maxDataCapacity = 0;

    private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

    // this flag is volatile so that data written by the writer in some thread is visible to a reader in another thread
    private volatile boolean m_writerClosed;

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    /**
     * @param delegate the delegate to which to write asynchronously
     * @param cache the cache for storing data
     * @param executor the executor to which to submit asynchronous writes to the delegate
     */
    public AsyncFlushCachedColumnStore(final ColumnStore delegate, final CachedColumnStoreCache cache,
        final ExecutorService executor) {
        m_delegate = delegate;
        m_factory = new AsyncFlushCachedColumnStoreFactory();
        m_writer = new AsyncFlushCachedColumnStoreWriter();
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
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public ColumnDataFactory getFactory() {
        if (m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_factory;
    }

    @Override
    public ColumnDataWriter getWriter() {
        if (m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_writer;
    }

    @Override
    public void save(final File file) throws IOException {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        try {
            waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
        }
        m_delegate.save(file);
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection selection) {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return new AsyncFlushCachedColumnStoreReader(selection);
    }

    @Override
    public void close() throws IOException {
        m_storeClosed = true;

        try {
            waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
        }

        for (final ColumnDataUniqueId id : m_cachedData.keySet()) {
            final ColumnReadData removed = m_globalCache.removeRetained(id);
            if (removed != null) {
                removed.release();
            }
        }
        m_cachedData.clear();
        m_delegate.close();
    }

}
