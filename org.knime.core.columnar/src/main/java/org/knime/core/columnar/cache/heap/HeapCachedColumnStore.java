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
package org.knime.core.columnar.cache.heap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.BatchWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.DelegatingColumnStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ColumnStore} intercepting {@link ObjectReadData} for in-heap caching of objects.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @since 4.3
 */
public final class HeapCachedColumnStore extends DelegatingColumnStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeapCachedColumnStore.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for serialization thread.";

    private final class HeapCachedBatchWriter extends DelegatingBatchWriter {

        private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

        private int m_numBatches;

        private HeapCachedBatchWriter() {
            super(HeapCachedColumnStore.this);
        }

        @Override
        protected WriteBatch createInternal(final int capacity) {
            final WriteBatch batch = super.createInternal(capacity);
            final NullableWriteData[] data = new NullableWriteData[getSchema().numColumns()];

            for (int i = 0; i < data.length; i++) {
                if (m_objectData.isSelected(i)) {
                    final ObjectWriteData<?> columnWriteData = (ObjectWriteData<?>)batch.get(i);
                    data[i] = new HeapCachedWriteData<>(columnWriteData);
                } else {
                    data[i] = batch.get(i);
                }
            }
            return new DefaultWriteBatch(data);
        }

        @Override
        protected void writeInternal(final ReadBatch batch) throws IOException {

            batch.retain();
            try {
                waitForPrevBatch();
            } catch (InterruptedException e) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
                LOGGER.info(ERROR_ON_INTERRUPT, e);
                return;
            }

            final int numColumns = batch.size();
            @SuppressWarnings("unchecked")
            final CompletableFuture<NullableReadData>[] futures = new CompletableFuture[numColumns];

            for (int i = 0; i < numColumns; i++) {
                if (m_objectData.isSelected(i)) {
                    final HeapCachedWriteData<?>.HeapCachedReadData heapCachedData =
                        (HeapCachedWriteData<?>.HeapCachedReadData)batch.get(i);
                    futures[i] = CompletableFuture.supplyAsync(heapCachedData::serialize, m_executor);
                    final ColumnDataUniqueId ccuid = new ColumnDataUniqueId(m_readStore, i, m_numBatches);
                    m_cache.put(ccuid, heapCachedData.getData());
                    m_cachedData.add(ccuid);
                } else {
                    futures[i] = CompletableFuture.completedFuture(batch.get(i));
                }
            }

            m_future = CompletableFuture.allOf(futures).thenRun(() -> { // NOSONAR
                try {
                    super.writeInternal(new DefaultReadBatch(
                        Arrays.stream(futures).map(CompletableFuture::join).toArray(NullableReadData[]::new)));
                } catch (IOException e) {
                    throw new IllegalStateException(String.format("Failed to write batch %d.", m_numBatches), e);
                } finally {
                    batch.release();
                }
            });

            m_numBatches++;
        }

        @Override
        protected void closeOnce() throws IOException {
            try {
                waitForPrevBatch();
            } catch (InterruptedException e) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
                LOGGER.info(ERROR_ON_INTERRUPT, e);
            }
            super.closeOnce();
        }

        private void waitForPrevBatch() throws InterruptedException, IOException {
            try {
                m_future.get();
            } catch (ExecutionException e) {
                throw new IOException("Failed to asynchronously serialize object data.", e);
            }
        }

    }

    private final ExecutorService m_executor;

    private final HeapCachedColumnReadStore m_readStore;

    private final ColumnSelection m_objectData;

    private final Map<ColumnDataUniqueId, Object[]> m_cache;

    private Set<ColumnDataUniqueId> m_cachedData = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * @param delegate the delegate to which to write
     * @param cache the in-heap cache for storing object data
     * @param executor the executor to which to submit asynchronous serialization tasks
     */
    public HeapCachedColumnStore(final ColumnStore delegate, final ObjectDataCache cache,
        final ExecutorService executor) {
        super(delegate);
        m_objectData = HeapCacheUtils.getObjectDataIndices(delegate.getSchema());
        m_readStore = new HeapCachedColumnReadStore(delegate, cache, m_cachedData);
        m_cache = cache.getCache();
        m_executor = executor;
    }

    @Override
    protected BatchWriter createWriterInternal() {
        return new HeapCachedBatchWriter();
    }

    @Override
    protected BatchReader createReaderInternal(final ColumnSelection config) {
        return m_readStore.createReader(config);
    }

    @Override
    protected void closeOnce() throws IOException {
        m_readStore.close();
        m_cachedData = null;
        super.closeOnce();
    }

}
