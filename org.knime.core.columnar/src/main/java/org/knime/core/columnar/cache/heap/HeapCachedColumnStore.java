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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.DelegatingColumnStore;

/**
 * {@link ColumnStore} interception {@link ObjectReadData} for in-heap caching of objects.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @since 4.3
 */
public final class HeapCachedColumnStore extends DelegatingColumnStore {

    private final class Factory extends DelegatingColumnDataFactory {

        private Factory() {
            super(HeapCachedColumnStore.this);
        }

        @Override
        protected WriteBatch createInternal(final int chunkSize) {
            final WriteBatch batch = super.createInternal(chunkSize);
            final ColumnWriteData[] data = new ColumnWriteData[getSchema().getNumColumns()];

            for (int i = 0; i < data.length; i++) {
                if (m_objectData.isSelected(i)) {
                    final ObjectWriteData<?> columnWriteData = (ObjectWriteData<?>)batch.get(i);
                    data[i] = new HeapCachedWriteReadData<>(columnWriteData);
                } else {
                    data[i] = batch.get(i);
                }
            }
            return new DefaultWriteBatch(data, batch.capacity());
        }

    }

    private final class Writer extends DelegatingColumnDataWriter {

        private Future<Void> m_serializationFuture = CompletableFuture.completedFuture(null);

        private int m_numBatches;

        private Writer() {
            super(HeapCachedColumnStore.this);
        }

        @Override
        protected void writeInternal(final ReadBatch batch) throws IOException {

            batch.retain();

            waitForPrevBatch();
            final List<CompletableFuture<?>> futures = new ArrayList<>();

            for (int i = 0; i < batch.getNumColumns(); i++) {
                if (m_objectData.isSelected(i)) {
                    final HeapCachedWriteReadData<?> heapCachedData = (HeapCachedWriteReadData<?>)batch.get(i);
                    futures.add(CompletableFuture.runAsync(heapCachedData::serialize, m_executor));
                    final ColumnDataUniqueId ccuid = new ColumnDataUniqueId(m_readStore, i, m_numBatches);
                    m_cache.put(ccuid, heapCachedData.getData());
                    m_cachedData.add(ccuid);
                }
            }

            m_serializationFuture =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRunAsync(() -> { // NOSONAR
                    try {
                        final ColumnReadData[] data = new ColumnReadData[batch.getNumColumns()];
                        for (int i = 0; i < data.length; i++) {
                            if (m_objectData.isSelected(i)) {
                                data[i] = ((HeapCachedWriteReadData<?>)batch.get(i)).getDelegate();
                            } else {
                                data[i] = batch.get(i);
                            }
                        }
                        super.writeInternal(new DefaultReadBatch(data, batch.length()));
                    } catch (IOException e) {
                        throw new IllegalStateException(String.format("Failed to write batch %d.", m_numBatches), e);
                    } finally {
                        batch.release();
                    }
                }, m_executor);
            m_numBatches++;
        }

        @Override
        protected void closeOnce() throws IOException {
            waitForPrevBatch();
            super.closeOnce();
        }

        private void waitForPrevBatch() {
            try {
                m_serializationFuture.get();
            } catch (InterruptedException e) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for serialization thread.", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Failed to asynchronously serialize object data.", e);
            }
        }

    }

    private final ExecutorService m_executor;

    private final HeapCachedColumnReadStore m_readStore;

    private final ColumnSelection m_objectData;

    private final Map<ColumnDataUniqueId, AtomicReferenceArray<?>> m_cache;

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
    protected ColumnDataFactory createFactoryInternal() {
        return new Factory();
    }

    @Override
    protected ColumnDataWriter createWriterInternal() {
        return new Writer();
    }

    @Override
    protected ColumnDataReader createReaderInternal(final ColumnSelection config) {
        return m_readStore.createReader(config);
    }

    @Override
    protected void closeOnce() throws IOException {
        m_readStore.close();
        m_cachedData = null;
        super.closeOnce();
    }

}
