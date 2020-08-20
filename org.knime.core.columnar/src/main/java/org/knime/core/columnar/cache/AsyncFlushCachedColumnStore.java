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

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.CachedColumnReadStore.CachedColumnReadStoreCache;
import org.knime.core.columnar.cache.CachedColumnReadStore.ColumnDataUniqueId;
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

    private static void waitForAndHandleFuture(final Future<Void> future) {
        try {
            future.get();
        } catch (final InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Failed to asynchronously write cached rows to file.", e);
        }
    }

    private static final AtomicLong THREAD_COUNT = new AtomicLong();

    static final ExecutorService EXECUTOR = Executors
        .newSingleThreadExecutor(r -> new Thread(r, "KNIME-BackgroundTableWriter-" + THREAD_COUNT.incrementAndGet()));

    private final class AsyncFlushCachedColumnStoreWriter implements ColumnDataWriter {

        private final ColumnDataWriter m_delegateWriter;

        /**
         * After a ColumnData is evicted (but before it is released), we check if we have to raise the maxEvictIndex.
         * For instance if we have evicted data at index 1 in the past and now evict data at index 3, we would raise the
         * maxEvictIndex by 2 from 1 to 3.
         */
        private final AtomicInteger m_maxEvictIndex = new AtomicInteger(-1);

        /**
         * We start at 0 permits. After one batch of ColumnData has been flushed, we gain (release) 1 permit. if we
         * raise the maxEvictIndex by i, we request i permits. Since we always flush sequentially, this assures that
         * whenever we evict unflushed data, we wait for the asynchronous writer thread to catch up.
         */
        private final Semaphore m_semaphore = new Semaphore(0);

        AsyncFlushCachedColumnStoreWriter() {
            m_delegateWriter = m_delegate.getWriter();
        }

        private final BiConsumer<ColumnDataUniqueId, ColumnData> m_evictor = (k, d) -> {
            final int index = k.getChunkIndex();
            final int maxEvictIndex = m_maxEvictIndex.getAndAccumulate(index, Math::max);
            if (index > maxEvictIndex) {
                try {
                    m_semaphore.acquire(index - maxEvictIndex);
                } catch (InterruptedException e) {
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                }
            }
        };

        @Override
        public void write(final ColumnData[] batch) throws IOException {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            handleDoneFutures();

            try {
                m_futures.add(EXECUTOR.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws IOException {
                        try {
                            m_delegateWriter.write(batch);
                        } finally {
                            m_semaphore.release();
                        }
                        return null;
                    }
                }));
            } catch (final RejectedExecutionException e) {
                m_semaphore.release();
                throw e;
            }

            m_readCache.addBatch(batch, m_evictor);
        }

        @Override
        public void close() {
            m_writerClosed = true;

            handleDoneFutures();

            m_futures.add(EXECUTOR.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    m_delegateWriter.close();
                    return null;
                }
            }));
        }

        private void handleDoneFutures() {
            while (!m_futures.isEmpty() && m_futures.peek().isDone()) {
                waitForAndHandleFuture(m_futures.remove());
            }
        }

    }

    private final ColumnStore m_delegate;

    private final ColumnDataFactory m_factory;

    private final AsyncFlushCachedColumnStoreWriter m_writer;

    private final CachedColumnReadStore m_readCache;

    private final Queue<Future<Void>> m_futures = new ConcurrentLinkedQueue<>();

    // this flag is volatile so that data written by the writer in some thread is visible to a reader in another thread
    private volatile boolean m_writerClosed;

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    /**
     * @param delegate the delegate to which to write asynchronously
     * @param cache the cache for storing data
     */
    public AsyncFlushCachedColumnStore(final ColumnStore delegate, final CachedColumnReadStoreCache cache) {
        m_delegate = delegate;
        m_factory = delegate.getFactory();
        m_writer = new AsyncFlushCachedColumnStoreWriter();
        m_readCache = new CachedColumnReadStore(delegate, cache);
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_readCache.getSchema();
    }

    @Override
    public ColumnDataFactory getFactory() {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_factory;
    }

    @Override
    public ColumnDataWriter getWriter() {
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

        waitForAndHandleFutures();
        m_delegate.saveToFile(file);
    }

    private void waitForAndHandleFutures() {
        Future<Void> future;
        while ((future = m_futures.poll()) != null) {
            waitForAndHandleFuture(future);
        }
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection selection) {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }

        return m_readCache.createReader(selection, this::waitForAndHandleFutures);
    }

    @Override
    public void close() throws IOException {
        m_storeClosed = true;

        while (!m_futures.isEmpty()) {
            m_futures.poll().cancel(true);
        }

        m_readCache.close();
        m_delegate.close();
    }

}
