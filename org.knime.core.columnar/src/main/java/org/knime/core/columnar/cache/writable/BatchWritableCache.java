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
package org.knime.core.columnar.cache.writable;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.EvictingCache;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link BatchWritable} and {@link RandomAccessBatchReadable} that stores all written {@link ReadData} in a
 * fixed-size {@link SharedBatchWritableCache LRU cache} in memory if the aggregated {@link ReferencedData#sizeOf()
 * size} of data is below a given threshold. If the threshold is exceeded or once evicted from the cache, the data is
 * passed on to a delegate.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class BatchWritableCache implements Flushable, BatchWritable, RandomAccessBatchReadable {

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for asynchronous write thread.";

    private static final String ERROR_MESSAGE_ON_FLUSH = "Error while flushing small table.";

    private final class BatchWritableCacheWriter implements BatchWriter {

        private final BatchWriter m_writerDelegate;

        private final CountDownLatch m_closed = new CountDownLatch(1);

        private final CountDownLatch m_delegateClosed = new CountDownLatch(1);

        private ReadBatches m_table = new ReadBatches();

        private boolean m_flushed;

        BatchWritableCacheWriter(final BatchWriter delegate) {
            m_writerDelegate = delegate;
        }

        @Override
        public WriteBatch create(final int capacity) {
            return m_writerDelegate.create(capacity);
        }

        @Override
        public synchronized void write(final ReadBatch batch) throws IOException {
            m_maxLength.accumulateAndGet(batch.length(), Math::max);
            m_numBatches.incrementAndGet();
            if (m_table != null) {
                m_table.retainAndAddBatch(batch);
                if (m_table.sizeOf() > m_smallTableThreshold) {
                    flush(m_table, false);
                    m_table.release();
                    m_table = null;
                }
            } else {
                m_writerDelegate.write(batch);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            try {
                if (m_table != null) {
                    m_globalCache.put(BatchWritableCache.this, m_table, (store, table) -> {
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
                        m_writerDelegate.close();
                    } finally {
                        m_delegateClosed.countDown();
                    }
                }
            } finally {
                m_closed.countDown();
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

        private synchronized void flush(final ReadBatches table, final boolean close) throws IOException {
            if (!m_flushed) {
                m_flushed = true;
                try {
                    for (int i = 0; i < table.getBatches().size(); i++) {
                        final ReadBatch batch = table.getBatch(i);
                        m_writerDelegate.write(batch);
                    }
                } finally {
                    if (close) {
                        try {
                            m_writerDelegate.close();
                        } finally {
                            m_delegateClosed.countDown();
                        }
                    }
                }
            }
        }

    }

    private static final class BatchWritableCacheReader implements RandomAccessBatchReader {

        private final ColumnSelection m_selection;

        private ReadBatches m_table;

        BatchWritableCacheReader(final ColumnSelection selection, final ReadBatches table) {
            m_selection = selection;
            m_table = table;
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            final ReadBatch batch = m_selection.createBatch(i -> m_table.getBatch(index).get(i));
            batch.retain();
            return batch;
        }

        @Override
        public synchronized void close() {
            if (m_table != null) {
                m_table.release();
                m_table = null;
            }
        }

    }

    private final BatchWritableCacheWriter m_writer;

    private final RandomAccessBatchReadable m_reabableDelegate;

    private final int m_smallTableThreshold;

    private final EvictingCache<BatchWritableCache, ReadBatches> m_globalCache;

    private final AtomicInteger m_numBatches = new AtomicInteger();

    private final AtomicInteger m_maxLength = new AtomicInteger();

    /**
     * @param delegate the delegate to which to write if the table is not small
     * @param cache the cache for obtaining and storing small tables
     */
    @SuppressWarnings("resource")
    public <D extends BatchWritable & RandomAccessBatchReadable> BatchWritableCache(final D delegate,
        final SharedBatchWritableCache cache) {
        m_writer = new BatchWritableCacheWriter(delegate.getWriter());
        m_reabableDelegate = delegate;
        m_globalCache = cache.getCache();
        m_smallTableThreshold = cache.getSizeThreshold();
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public void flush() throws IOException {
        // if writer unclosed
        if (m_writer.m_closed.getCount() == 1) {
            // nothing to flush
            return;
        }

        final ReadBatches cached = m_globalCache.getRetained(BatchWritableCache.this);
        if (cached != null) {
            m_writer.flush(cached, true);
            cached.release();
        } else {
            m_writer.waitForDelegateWriter();
        }
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        // Wait for writer to be closed. While the writer is not closed, neither will the table have been placed in the
        // cache nor will the delegate writer have been closed.
        // TODO: this needs review when this class is adjusted to allow reading while writing (AP-15959)
        try {
            m_writer.m_closed.await();
        } catch (InterruptedException e) {
            // Restore interrupted state
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ERROR_ON_INTERRUPT, e);
        }

        final ReadBatches cached = m_globalCache.getRetained(BatchWritableCache.this);
        if (cached != null) {
            // cache hit
            return new BatchWritableCacheReader(selection, cached);
        } else {
            // cache miss
            // we are not putting the small table back into cache here for two reasons:
            // (1) the CachedColumnStore which this store usually delegates to will already so so
            // (2) it might not be read fully by the reader, so by putting it back into the cache, we would read
            // unnecessarily much data
            m_writer.waitForDelegateWriter();
            return m_reabableDelegate.createRandomAccessReader(selection);
        }
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_reabableDelegate.getSchema();
    }

    @Override
    public synchronized void close() throws IOException {
        final ReadBatches removed = m_globalCache.removeRetained(BatchWritableCache.this);
        if (removed != null) {
            removed.release();
        }
        m_reabableDelegate.close();
    }

}
