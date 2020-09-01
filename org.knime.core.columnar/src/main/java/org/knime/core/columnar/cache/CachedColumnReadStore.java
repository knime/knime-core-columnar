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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.LoadingEvictingCache.Evictor;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnReadStore} that reads {@link ColumnData} from a delegate column store and places it in a fixed-size
 * {@link SmallColumnStoreCache LRU cache} in memory for faster subsequent access. The store allows concurrent reading
 * via multiple {@link ColumnDataReader ColumnDataReaders}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class CachedColumnReadStore implements ColumnReadStore {

    private static final Object DUMMY = new Object();

    private final class CachedColumnStoreReader implements ColumnDataReader {

        private final ColumnDataReader m_delegateReader;

        private final int[] m_indices;

        private final BufferedBatchLoader m_batchLoader;

        private final Evictor<ColumnDataUniqueId, ColumnData> m_evictor = (k, c) -> m_cachedData.remove(k);

        private boolean m_readerClosed;

        CachedColumnStoreReader(final ColumnSelection selection) {
            m_delegateReader = m_delegate.createReader(selection);
            m_indices = selection != null && selection.get() != null ? selection.get()
                : IntStream.range(0, getSchema().getNumColumns()).toArray();
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

            for (int i : m_indices) {
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(CachedColumnReadStore.this, i, chunkIndex);
                batch[i] = m_globalCache.getRetained(ccUID, () -> {
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
            return m_delegateReader.getNumChunks();
        }

        @Override
        public int getMaxDataCapacity() {
            return m_delegateReader.getMaxDataCapacity();
        }

        @Override
        public void close() throws IOException {
            m_delegateReader.close();
            m_batchLoader.close();
            m_readerClosed = true;
        }

    }

    private final ColumnReadStore m_delegate;

    private final LoadingEvictingCache<ColumnDataUniqueId, ColumnData> m_globalCache;

    private final Map<ColumnDataUniqueId, Object> m_cachedData = new ConcurrentHashMap<>();

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    /**
     * @param delegate the delegate from which to read in case of a cache miss
     * @param cache the cache for obtaining and storing data
     */
    public CachedColumnReadStore(final ColumnReadStore delegate, final CachedColumnStoreCache cache) {
        m_delegate = delegate;
        m_globalCache = cache.getCache();
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection selection) {
        return new CachedColumnStoreReader(selection);
    }

    @Override
    public void close() {
        for (final ColumnDataUniqueId id : m_cachedData.keySet()) {
            final ColumnData removed = m_globalCache.removeRetained(id);
            if (removed != null) {
                removed.release();
            }
        }
        m_cachedData.clear();
        m_storeClosed = true;
    }

}
