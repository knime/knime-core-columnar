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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.BatchWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.DelegatingColumnStore;

/**
 * A {@link ColumnStore} that stores {@link NullableWriteData} in a fixed-size {@link SmallColumnStoreCache LRU cache}
 * in memory if the aggregated {@link ReferencedData#sizeOf() size} of data is below a given threshold. If the threshold
 * is exceeded or once evicted from the cache, the data is passed on to a delegate column store. The store allows
 * concurrent reading via multiple {@link BatchReader ColumnDataReaders} once the {@link BatchWriter} has been closed.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class SmallColumnStore extends DelegatingColumnStore {

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for asynchronous write thread.";

    private static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    /**
     * A cache for storing small tables that can be shared between multiple {@link SmallColumnStore SmallColumnStores}.
     */
    public static final class SmallColumnStoreCache {

        private final int m_smallTableThreshold;

        private final EvictingCache<SmallColumnStore, Table> m_cache;

        private final long m_cacheSize;

        /**
         * @param smallTableThreshold the size (in bytes) that determines whether a table is considered small
         * @param cacheSize the number of small tables the cache should be able to hold
         * @param concurrencyLevel the allowed concurrency among update operations
         */
        public SmallColumnStoreCache(final int smallTableThreshold, final long cacheSize, final int concurrencyLevel) {
            m_smallTableThreshold = smallTableThreshold;
            m_cache = new SizeBoundLruCache<>(cacheSize, concurrencyLevel);
            m_cacheSize = cacheSize;
        }

        /**
         * @return maximum number of small tables to be managed by cache.
         */
        public final long getMaxSize() {
            return m_cacheSize;
        }

        int size() {
            return m_cache.size();
        }
    }

    private static final class Table implements ReferencedData {

        private final List<ReadBatch> m_batches = Collections.synchronizedList(new ArrayList<>());

        private final AtomicLong m_sizeOf = new AtomicLong();

        private final AtomicInteger m_maxDataCapacity = new AtomicInteger();

        void retainAndAddBatch(final ReadBatch batch) {
            batch.retain();
            m_sizeOf.addAndGet(batch.sizeOf());
            m_maxDataCapacity.accumulateAndGet(batch.length(), Math::max);
            m_batches.add(batch);
        }

        ReadBatch getBatch(final int index) {
            return m_batches.get(index);
        }

        @Override
        public void release() {
            synchronized (m_batches) {
                for (final ReadBatch batch : m_batches) {
                    batch.release();
                }
            }
        }

        @Override
        public void retain() {
            synchronized (m_batches) {
                for (final ReadBatch batch : m_batches) {
                    batch.retain();
                }
            }
        }

        @Override
        public long sizeOf() {
            return m_sizeOf.get();
        }

        int getMaxDataCapacity() {
            return m_maxDataCapacity.get();
        }

        int getNumChunks() {
            return m_batches.size();
        }

    }

    private static final String ERROR_MESSAGE_ON_FLUSH = "Error while flushing small table.";

    private final class SmallBatchWriter extends DelegatingBatchWriter {

        private Table m_table = new Table();

        private final CountDownLatch m_delegateClosed = new CountDownLatch(1);

        private boolean m_flushed = false;

        SmallBatchWriter() {
            super(SmallColumnStore.this);
        }

        @Override
        protected void writeInternal(final ReadBatch batch) throws IOException {
            if (m_table != null) {
                m_table.retainAndAddBatch(batch);
                if (m_table.sizeOf() > m_smallTableThreshold) {
                    flush(m_table, false);
                    m_table.release();
                    m_table = null;
                }
            } else {
                super.writeInternal(batch);
            }
        }

        @Override
        protected void closeOnce() throws IOException {
            if (m_table != null) {
                m_globalCache.put(SmallColumnStore.this, m_table, (store, table) -> {
                    try {
                        flush(table, true);
                    } catch (IOException e) {
                        throw new IllegalStateException(ERROR_MESSAGE_ON_FLUSH, e);
                    }
                });
                m_table.release(); // from here on out, the cache is responsible for retaining
                m_table = null;
            } else {
                try {
                    @SuppressWarnings("resource")
                    final BatchWriter delegate = initAndGetDelegate();
                    delegate.close();
                } finally {
                    m_delegateClosed.countDown();
                }
            }
        }

        private void waitForDelegateWriter() {
            try {
                m_delegateClosed.await();
            } catch (InterruptedException e) {
                // Restore interrupted state
                Thread.currentThread().interrupt();
                throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
            }
        }

        private synchronized void flush(final Table table, final boolean close) throws IOException {
            if (!m_flushed) {
                m_flushed = true;
                try {
                    for (int i = 0; i < table.getNumChunks(); i++) {
                        final ReadBatch batch = table.getBatch(i);
                        @SuppressWarnings("resource")
                        final BatchWriter delegate = initAndGetDelegate();
                        delegate.write(batch);
                    }
                    @SuppressWarnings("resource")
                    final BatchWriter delegate = getDelegate();
                    if (close && delegate != null) {
                        delegate.close();
                    }
                } finally {
                    if (close) {
                        m_delegateClosed.countDown();
                    }
                }
            }
        }

    }

    private final class SmallBatchReader implements BatchReader {

        private final ColumnSelection m_selection;

        private Table m_table;

        private boolean m_readerClosed;

        SmallBatchReader(final ColumnSelection selection, final Table table) {
            m_selection = selection;
            m_table = table;
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }
            if (index < 0) {
                throw new IndexOutOfBoundsException(String.format("Batch index %d smaller than 0.", index));
            }
            if (index >= numBatches()) {
                throw new IndexOutOfBoundsException(
                    String.format("Batch index %d greater or equal to the reader's largest batch index (%d).", index,
                        numBatches() - 1));
            }

            final ReadBatch batch = m_selection.createBatch(i -> m_table.getBatch(index).get(i));
            batch.retain();
            return batch;
        }

        @Override
        public void close() {
            m_readerClosed = true;
            if (m_table != null) {
                m_table.release();
                m_table = null;
            }
        }

        @Override
        public int numBatches() {
            return m_table.getNumChunks();
        }

        @Override
        public int maxLength() {
            return m_table.getMaxDataCapacity();
        }

    }

    private final int m_smallTableThreshold;

    private final EvictingCache<SmallColumnStore, Table> m_globalCache;

    // lazily initialized on flush
    private SmallBatchWriter m_writer;

    /**
     * @param delegate the delegate to which to write if the table is not small
     * @param cache the cache for obtaining and storing small tables
     */
    public SmallColumnStore(final ColumnStore delegate, final SmallColumnStoreCache cache) {
        super(delegate);
        m_globalCache = cache.m_cache;
        m_smallTableThreshold = cache.m_smallTableThreshold;
    }

    @Override
    protected BatchWriter createWriterInternal() {
        m_writer = new SmallBatchWriter();
        return m_writer;
    }

    @Override
    protected void saveInternal(final File f) throws IOException {
        final Table cached = m_globalCache.getRetained(SmallColumnStore.this);
        if (cached != null) {
            m_writer.flush(cached, true);
            cached.release();
        } else {
            m_writer.waitForDelegateWriter();
        }

        super.saveInternal(f);
    }

    @SuppressWarnings("resource")
    @Override
    protected BatchReader createReaderInternal(final ColumnSelection selection) {
        final Table cached = m_globalCache.getRetained(SmallColumnStore.this);
        if (cached != null) {
            // cache hit
            return new SmallBatchReader(selection, cached);
        } else {
            // cache miss
            // we are not putting the small table back into cache here for two reasons:
            // (1) the CachedColumnStore which this store usually delegates to will already so so
            // (2) it might not be read fully by the reader, so by putting it back into the cache, we would read
            // unnecessarily much data
            m_writer.waitForDelegateWriter();
            return getDelegate().createReader(selection);
        }
    }

    @Override
    protected void closeOnce() throws IOException {
        final Table removed = m_globalCache.removeRetained(SmallColumnStore.this);
        if (removed != null) {
            removed.release();
        }
        super.closeOnce();
    }

}
