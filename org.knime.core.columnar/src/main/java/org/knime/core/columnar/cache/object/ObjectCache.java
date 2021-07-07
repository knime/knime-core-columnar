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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchWritable} and {@link RandomAccessBatchReadable} that intercepts {@link VarBinaryReadData} for in-heap
 * caching of objects.
 *
 * It performs asynchronous work on two different executors. The persist executor is used to asynchronously store data
 * in the cache during writing. The serialization executor handles asynchronous serialization of in-heap
 * data into the delegate batch writer.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class ObjectCache implements BatchWritable, RandomAccessBatchReadable, Flushable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectCache.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for serialization thread.";

    private static final String ERROR_ON_READABLE_CLOSE = "Error while closing readable delegate.";

    private final class ObjectCacheWriter implements BatchWriter {

        private final BatchWriter m_writerDelegate;

        private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

        /**
         * The number of batches that are already written. Readers use this value to determine if they are
         * accessing the currently written batch and need to wait for the serialization to finish.
         * To make sure numBatches is up to date when a Reader accesses it, we make it volatile.
         */
        private volatile int m_numBatches;

        private ObjectCacheWriter(final BatchWriter delegate) {
            m_writerDelegate = delegate;
        }

        @Override
        public WriteBatch create(final int capacity) {
            final WriteBatch batch = m_writerDelegate.create(capacity);
            final NullableWriteData[] data = new NullableWriteData[getSchema().numColumns()];

            for (int i = 0; i < data.length; i++) {
                if (m_varBinaryData.isSelected(i)) {
                    final VarBinaryWriteData columnWriteData = (VarBinaryWriteData)batch.get(i);
                    final var cachedData =
                        new CachedVarBinaryWriteData(columnWriteData, m_serializationExecutor);
                    m_unclosedData.add(cachedData);
                    data[i] = cachedData;
                } else if (m_stringData.isSelected(i)) {
                    final StringWriteData columnWriteData = (StringWriteData)batch.get(i);
                    final var cachedData = new CachedStringWriteData(columnWriteData, m_serializationExecutor);
                    m_unclosedData.add(cachedData);
                    data[i] = cachedData;
                } else {
                    data[i] = batch.get(i);
                }
            }
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
                // Restore interrupted state...
                Thread.currentThread().interrupt();
                LOGGER.error(ERROR_ON_INTERRUPT, e);
            }

            final int numColumns = batch.numData();
            @SuppressWarnings("unchecked")
            final CompletableFuture<NullableReadData>[] futures = new CompletableFuture[numColumns];

            for (int i = 0; i < numColumns; i++) {
                if (m_varBinaryData.isSelected(i) || m_stringData.isSelected(i)) {
                    final var heapCachedData = (CachedWriteData<?, ?, ?>.CachedReadData)batch.get(i);
                    futures[i] = CompletableFuture.supplyAsync(() -> {
                        m_unclosedData.remove(heapCachedData.getWriteData());
                        return heapCachedData.closeWriteDelegate();
                    }, m_executor);
                    final ColumnDataUniqueId ccuid = new ColumnDataUniqueId(ObjectCache.this, i, m_numBatches);
                    m_cache.getCache().put(ccuid, heapCachedData.getData());
                    m_cachedData.add(ccuid);
                } else {
                    futures[i] = CompletableFuture.completedFuture(batch.get(i));
                }
            }

            int currentBatch = m_numBatches;
            m_future = CompletableFuture.allOf(futures).thenRunAsync(() -> { // NOSONAR
                try {
                    m_writerDelegate.write(new DefaultReadBatch(
                        Arrays.stream(futures).map(CompletableFuture::join).toArray(NullableReadData[]::new)));
                } catch (IOException e) {
                    throw new IllegalStateException(String.format("Failed to write batch %d.", currentBatch), e);
                } finally {
                    batch.release();
                }
            }, m_executor);

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
            }, m_executor);
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

        private final ColumnSelection m_selection;

        private ObjectCacheReader(final ColumnSelection selection) {
            m_readerDelegate = m_readableDelegate.createRandomAccessReader(selection);
            m_selection = selection;
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
                    LOGGER.error(ERROR_ON_INTERRUPT, e);
                }
            }

            final ReadBatch batch = m_readerDelegate.readRetained(index);
            return m_selection.createBatch(i -> {
                if (m_varBinaryData.isSelected(i)) {
                    return wrapVarBinary(batch, index, i);
                } else if (m_stringData.isSelected(i)) {
                    return wrapString(batch, index, i);
                } else {
                    return batch.get(i);
                }
            });
        }

        private CachedVarBinaryLoadingReadData wrapVarBinary(final ReadBatch batch, final int batchIndex,
            final int columnIndex) {
            final VarBinaryReadData columnReadData = (VarBinaryReadData)batch.get(columnIndex);
            final Object[] array =
                m_cache.getCache().computeIfAbsent(new ColumnDataUniqueId(ObjectCache.this, columnIndex, batchIndex),
                    k -> new Object[columnReadData.length()]);
            return new CachedVarBinaryLoadingReadData(columnReadData, array);
        }

        private CachedStringLoadingReadData wrapString(final ReadBatch batch, final int batchIndex,
            final int columnIndex) {
            final StringReadData columnReadData = (StringReadData)batch.get(columnIndex);
            final String[] array = (String[])m_cache.getCache().computeIfAbsent(
                new ColumnDataUniqueId(ObjectCache.this, columnIndex, batchIndex),
                k -> new String[columnReadData.length()]);
            return new CachedStringLoadingReadData(columnReadData, array);
        }

        @Override
        public void close() throws IOException {
            m_readerDelegate.close();
        }

    }

    private final ObjectCacheWriter m_writer;

    private final ExecutorService m_executor;

    private final ExecutorService m_serializationExecutor;

    private final RandomAccessBatchReadable m_readableDelegate;

    private final ColumnSelection m_varBinaryData;

    private final ColumnSelection m_stringData;

    private final SharedObjectCache m_cache;

    private final Set<CachedWriteData<?, ?, ?>> m_unclosedData = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private Set<ColumnDataUniqueId> m_cachedData = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    /**
     * @param delegate the delegate to which to write
     * @param cache the in-heap cache for storing object data
     * @param persistExecutor the executor to which to submit asynchronous persist tasks
     * @param serializationExecutor the executor to use for asynchronous serialization of data
     */
    @SuppressWarnings("resource")
    public <D extends BatchWritable & RandomAccessBatchReadable> ObjectCache(final D delegate,
        final SharedObjectCache cache, final ExecutorService persistExecutor,
        final ExecutorService serializationExecutor) {
        m_writer = new ObjectCacheWriter(delegate.getWriter());
        m_varBinaryData = HeapCacheUtils.getVarBinaryIndices(delegate.getSchema());
        m_stringData = HeapCacheUtils.getStringIndices(delegate.getSchema());
        m_readableDelegate = delegate;
        m_cache = cache;
        m_executor = persistExecutor;
        m_serializationExecutor = serializationExecutor;
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

    private synchronized void writeUnclosedData() {
        for (CachedWriteData<?, ?, ?> data : m_unclosedData) {
            data.flush();
        }
    }

    @Override
    public void flush() throws IOException {
        // If we are closed, then all serialization has been dispatched and we only need to wait for it.
        // Flush can be invoked by a MemoryAlert while or after close().
        if (!m_closed.get()) {
            // serialize any currently unclosed data (in the current batch)
            writeUnclosedData();
        }

        // wait for the pending serialization of the previous batch
        try {
            m_writer.waitForAndHandleFuture();
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            LOGGER.error(ERROR_ON_INTERRUPT, e);
        }

    }

    /**
     * Asynchronously waits for the current write operations to finish and then
     * disposes of the cache resources and closes its delegate.
     *
     * Does not block!
     */
    @Override
    public synchronized void close() throws IOException {
        m_closed.set(true);
        m_writer.close();

        m_writer.m_future = m_writer.m_future.thenRunAsync(this::deferredClose);
        m_writer.handleDoneFuture();
    }

    private void deferredClose() {
        m_unclosedData.clear();
        m_cache.getCache().keySet().removeAll(m_cachedData);
        m_cachedData.clear();
        m_cachedData = null;

        try {
            m_readableDelegate.close();
        } catch (IOException ex) {
            LOGGER.error(ERROR_ON_READABLE_CLOSE, ex);
        }
    }
}
