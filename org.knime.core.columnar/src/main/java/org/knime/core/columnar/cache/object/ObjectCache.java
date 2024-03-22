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
 */
package org.knime.core.columnar.cache.object;

import java.io.Flushable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.CachedData.CachedDataFactory;
import org.knime.core.columnar.cache.object.CachedData.CachedWriteData;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchWritable} and {@link RandomAccessBatchReadable} that intercepts {@link VarBinaryReadData} for in-heap
 * caching of objects.
 *
 * It performs asynchronous work on two different executors. The persist executor is used to asynchronously store data
 * in the cache during writing. The serialization executor handles asynchronous serialization of in-heap data into the
 * delegate batch writer.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class ObjectCache implements BatchWritable, RandomAccessBatchReadable, Flushable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectCache.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for serialization thread.";

    private final ObjectCacheWriter m_writer;

    private final ExecutorService m_persistExecutor;

    private final RandomAccessBatchReadable m_readableDelegate;

    private final CountUpDownLatch m_serializationLatch = new CountUpDownLatch(1);

    private final Set<CachedWriteData> m_unclosedData = ConcurrentHashMap.newKeySet();

    private final CacheManager m_cacheManager;

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    private final CachedDataFactory[] m_cachedDataFactories;

    /**
     * @param writable the delegate to which to write
     * @param readable the delegate from which to read
     * @param cache the in-heap cache for storing object data
     * @param persistExecutor the executor to which to submit asynchronous persist tasks
     * @param serializationExecutor the executor to use for asynchronous serialization of data
     */
    @SuppressWarnings("resource")
    public ObjectCache(final BatchWritable writable, final RandomAccessBatchReadable readable,
        final SharedObjectCache cache, final ExecutorService persistExecutor,
        final ExecutorService serializationExecutor) {
        m_writer = new ObjectCacheWriter(writable.getWriter());
        m_readableDelegate = readable;
        m_persistExecutor = persistExecutor;
        m_cacheManager = new CacheManager(cache);
        m_cachedDataFactories = CachedDataFactoryBuilder.createForWriting(m_persistExecutor, m_unclosedData,
            m_cacheManager, m_serializationLatch, serializationExecutor).build(getSchema());
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new ObjectCacheReader(selection);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_readableDelegate.getSchema();
    }

    @Override
    public synchronized void flush() throws IOException {
        // serialize any currently unclosed data (in the current batch)
        for (var data : m_unclosedData) {
            data.flush();
        }

        // wait for the pending serialization of the previous batch
        try {
            m_writer.waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Wait for the current write operations to finish and then dispose of the cache resources and close the delegate.
     *
     * Blocks until closed!
     */
    @Override
    public synchronized void close() throws IOException {
        if (m_closed.getAndSet(true)) {
            return;
        }

        // if the cache is closed without a flush, then the CachedWriteDatas might not have released their resources
        // yet and we need to wait for that to happen
        m_unclosedData.forEach(CachedWriteData::cancel);
        m_unclosedData.clear();

        // wait for all registered serialization tasks to finish
        m_serializationLatch.countDown();
        m_serializationLatch.await();

        m_writer.close();
        try {
            m_writer.waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Note: we're not restoring interrupted state here, close should not be interrupted. Instead we retry waiting.
            try {
                m_writer.waitForAndHandleFuture();
            } catch (InterruptedException f) {
                throw new IOException("Closing the cache got interrupted while waiting for writer to finish.");
            }
        }

        m_cacheManager.close();

        m_readableDelegate.close();
    }

    private ColumnDataUniqueId createId(final int columnIndex, final int batchIndex) {
        return new ColumnDataUniqueId(this, columnIndex, batchIndex);
    }

    private final class ObjectCacheWriter implements BatchWriter {

        private final BatchWriter m_writerDelegate;

        private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

        /**
         * The number of batches that are already written. Readers use this value to determine if they are accessing the
         * currently written batch and need to wait for the serialization to finish. To make sure numBatches is up to
         * date when a Reader accesses it, we make it volatile.
         */
        private volatile int m_numBatches;

        private volatile int m_numCreatedBatches;

        private ObjectCacheWriter(final BatchWriter delegate) {
            m_writerDelegate = delegate;
        }

        @Override
        public WriteBatch create(final int capacity) {
            final WriteBatch batch = m_writerDelegate.create(capacity);
            var schema = getSchema();
            final var data = new NullableWriteData[schema.numColumns()];
            final int currentBatch = m_numCreatedBatches;
            m_numCreatedBatches++;
            Arrays.setAll(data, i -> m_cachedDataFactories[i].createWriteData(batch.get(i), createId(i, currentBatch)));
            return new DefaultWriteBatch(data);
        }

        @Override
        public synchronized void write(final ReadBatch batch) throws IOException {

            /* We have to retain this batch, since we are passing it onward to the delegate reader asynchronously. We
             * then wait for the previous (retained, but not yet delegated to the underlying writer) batch. If we would
             * not wait and if the serialization threads are lagging behind the main thread filling the batches, we
             * would end up with multiple retained batches. While the heap memory is monitored by KNIME's memory alert
             * system, the off-heap region is not, so we could run out of off-heap memory eventually. Note that the
             * underlying ReadDataCache, which caches batches off-heap and makes sure not to take up too much off-heap
             * memory, does not have this batch cached yet. If we wanted to cache it, we would have to invoke
             * Batch::sizeOf. This, in turn, would lead to serialization of the data, so we would still have to wait. */
            batch.retain();
            try {
                waitForAndHandleFuture();
            } catch (InterruptedException e) {
                batch.release();
                // Restore interrupted state and return from function. If writing was interrupted
                // we don't need to start writing the next batch!
                Thread.currentThread().interrupt();
                return;
            }

            final int numColumns = batch.numData();
            final var futures = new CompletableFuture[numColumns];
            int currentBatch = m_numBatches;
            Arrays.setAll(futures,
                i -> m_cachedDataFactories[i].getCachedDataFuture(batch.get(i)));
            m_future = CompletableFuture.allOf(futures).thenRunAsync(() -> { // NOSONAR
                var serializedBatch = batch.decorate((i, d) -> (NullableReadData)futures[i].join());
                try {
                    m_writerDelegate.write(serializedBatch);
                } catch (IOException e) {
                    throw new IllegalStateException(String.format("Failed to write batch %d.", currentBatch), e);
                } finally {
                    batch.release();
                }
            }, m_persistExecutor);

            m_numBatches++;
        }

        @Override
        public synchronized void close() throws IOException {
            handleDoneFuture();

            m_future = m_future.thenRunAsync(() -> {
                try {
                    m_writerDelegate.close();
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to close writer.", e);
                }
            }, m_persistExecutor);
        }

        private void waitForAndHandleFuture() throws InterruptedException, IOException {
            try {
                m_future.get();
            } catch (ExecutionException e) {
                throw new IOException("Failed to asynchronously serialize object data.", e);
            }
        }

        private void handleDoneFuture() throws IOException {
            if (m_future.isDone()) {
                try {
                    waitForAndHandleFuture();
                } catch (InterruptedException e) {
                    // Restore interrupted state
                    Thread.currentThread().interrupt();
                    // since we just checked whether the future is done, we likely never end up in this code block
                    LOGGER.info(ERROR_ON_INTERRUPT, e);
                }
            }
        }

    }

    private final class ObjectCacheReader implements RandomAccessBatchReader {

        private final RandomAccessBatchReader m_readerDelegate;

        private ObjectCacheReader(final ColumnSelection selection) {
            m_readerDelegate = m_readableDelegate.createRandomAccessReader(selection);
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            // when reading the last batch, we'll have to wait for that last batch to be serialized
            if (index == m_writer.m_numBatches - 1) {
                try {
                    m_writer.waitForAndHandleFuture();
                } catch (InterruptedException e) {
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                }
            }

            final ReadBatch batch = m_readerDelegate.readRetained(index);
            return batch.decorate((i, d) -> m_cachedDataFactories[i].createReadData(d, createId(i, index)));
        }

        @Override
        public void close() throws IOException {
            m_readerDelegate.close();
        }

    }

    @Override
    public long[] getBatchBoundaries() {
        return m_readableDelegate.getBatchBoundaries();
    }
}
