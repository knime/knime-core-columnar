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

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_READER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.LoadingEvictingCache.Evictor;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnStore column store} that stores {@link ColumnData data} in a fixed-size {@link SmallColumnStoreCache
 * LRU cache} in memory and asynchronously passes (flushes) data on to a delegate column store. When any unflushed data
 * is evicted from the cache, the store blocks until that data has been flushed. On cache miss (i.e., when any evicted,
 * flushed data is read), it blocks until the asynchronous write process has fully terminated. The store allows
 * concurrent reading via multiple {@link ColumnDataReader readers} once the {@link ColumnDataWriter writer} has been
 * closed.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class AsyncFlushCachedColumnStore implements ColumnStore {

    private static final String ERROR_ON_INTERRUPT = "Interrupted while writing for asynchronous write thread.";

    private static final CountDownLatch DUMMY = new CountDownLatch(0);

    private final class AsyncFlushCachedColumnStoreWriter implements ColumnDataWriter {

        private final ColumnDataWriter m_delegateWriter;

        AsyncFlushCachedColumnStoreWriter() {
            m_delegateWriter = m_delegate.getWriter();
        }

        @Override
        public void write(final ColumnData[] batch) throws IOException {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            handleDoneFuture();

            final CountDownLatch batchFlushed = new CountDownLatch(1);
            enqueueRunnable(() -> {
                try {
                    m_delegateWriter.write(batch);
                } catch (IOException e) {
                    throw new IllegalStateException(String.format("Failed to write batch %d.", m_numChunks), e);
                } finally {
                    batchFlushed.countDown();
                }
            });

            for (int i = 0; i < batch.length; i++) {
                m_maxDataCapacity = Math.max(m_maxDataCapacity, batch[i].getMaxCapacity());
                final ColumnDataUniqueId ccUID =
                    new ColumnDataUniqueId(AsyncFlushCachedColumnStore.this, i, m_numChunks);
                m_cachedData.put(ccUID, batchFlushed);
                m_globalCache.put(ccUID, batch[i], m_evictor);
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
                } catch (InterruptedException ex) {
                    // Restore interrupted state
                    Thread.currentThread().interrupt();
                    // since we just checked whether the future is done, we likely never end up in this code block
                }
            }
        }

    }

    private final class AsyncFlushCachedColumnStoreReader implements ColumnDataReader {

        private final ColumnSelection m_selection;

        private final int[] m_indices;

        private final BufferedBatchLoader m_batchLoader;

        // lazily initialized
        private ColumnDataReader m_delegateReader;

        private boolean m_readerClosed;

        AsyncFlushCachedColumnStoreReader(final ColumnSelection selection) {
            m_indices = selection != null && selection.get() != null ? selection.get()
                : IntStream.range(0, getSchema().getNumColumns()).toArray();
            m_selection = selection;
            m_batchLoader = new BufferedBatchLoader(m_indices);
        }

        @Override
        public ColumnData[] read(final int chunkIndex) {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            final ColumnData[] batch = new ColumnData[getSchema().getNumColumns()];

            for (final int i : m_indices) {
                final ColumnDataUniqueId ccUID =
                    new ColumnDataUniqueId(AsyncFlushCachedColumnStore.this, i, chunkIndex);
                batch[i] = m_globalCache.getRetained(ccUID, () -> {
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
                        final ColumnData data = m_batchLoader.loadBatch(m_delegateReader, chunkIndex)[i];
                        data.retain();
                        return data;
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }, m_evictor);
            }

            return batch;
        }

        @Override
        public int getNumChunks() {
            return m_numChunks;
        }

        @Override
        public int getMaxDataCapacity() {
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

    private final LoadingEvictingCache<ColumnDataUniqueId, ColumnData> m_globalCache;

    private final Map<ColumnDataUniqueId, CountDownLatch> m_cachedData = new ConcurrentHashMap<>();

    private final Evictor<ColumnDataUniqueId, ColumnData> m_evictor = (k, c) -> {
        try {
            m_cachedData.remove(k).await();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            // when interrupted here (e.g., because some other node that pushes data into the cache is cancelled), we
            // either lose the unflushed, to-be-evicted data, because it will be released once evicted from the cache
            // or we retain it and accept that we temporarily use up more off-heap memory than the cache allows
            c.retain();
            enqueueRunnable(c::release);
            throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
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
        m_factory = delegate.getFactory();
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
    public void saveToFile(final File file) throws IOException {
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
        m_delegate.saveToFile(file);
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

        m_future.cancel(true);

        for (final ColumnDataUniqueId id : m_cachedData.keySet()) {
            final ColumnData removed = m_globalCache.removeRetained(id);
            if (removed != null) {
                removed.release();
            }
        }
        m_cachedData.clear();
        m_delegate.close();
    }

}
