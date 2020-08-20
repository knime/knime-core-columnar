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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.ColumnStoreSchema;
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

    /**
     * A cache for storing data that can be shared between multiple {@link CachedColumnReadStore
     * CachedColumnReadStores}.
     */
    public static final class CachedColumnReadStoreCache {

        private final LoadingEvictingCache<ColumnDataUniqueId, ColumnData> m_cache;

        /**
         * @param cacheSize the size of the cache in bytes
         */
        public CachedColumnReadStoreCache(final long cacheSize) {
            m_cache = new SizeBoundLruCache<>(cacheSize);
        }

        int size() {
            return m_cache.size();
        }
    }

    private final class CachedColumnStoreReader implements ColumnDataReader {

        private final class Loader implements Function<ColumnDataUniqueId, ColumnData>, AutoCloseable {

            private ColumnData[] m_loadedData;

            @Override
            public ColumnData apply(final ColumnDataUniqueId ccUID) {
                if (m_loadedData == null) {
                    try {
                        initDelegateReader();
                        m_onUseDelegateReader.run();
                        m_loadedData = m_delegateReader.read(ccUID.m_chunkIndex);
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }
                m_inCache.put(ccUID, DUMMY);
                m_loadedData[ccUID.m_columnIndex].retain();
                return m_loadedData[ccUID.m_columnIndex];
            }

            @Override
            public void close() {
                if (m_loadedData != null) {
                    final int[] indices = m_selection != null ? m_selection.get() : null;
                    final int numRequested = indices != null ? indices.length : getSchema().getNumColumns();
                    for (int i = 0; i < numRequested; i++) {
                        final int colIndex = indices != null ? indices[i] : i;
                        m_loadedData[colIndex].release();
                    }
                }
            }
        }

        private final ColumnSelection m_selection;

        private final Runnable m_onUseDelegateReader;

        // lazily initialized
        private ColumnDataReader m_delegateReader;

        private boolean m_readerClosed;

        CachedColumnStoreReader(final ColumnSelection selection, final Runnable onUseDelegateReader) {
            m_selection = selection;
            m_onUseDelegateReader = onUseDelegateReader;
        }

        private void initDelegateReader() {
            if (m_delegateReader == null) {
                m_delegateReader = m_delegate.createReader(m_selection);
            }
        }

        @Override
        public ColumnData[] read(final int chunkIndex) {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            final int[] indices = m_selection != null ? m_selection.get() : null;
            final int numRequested = indices != null ? indices.length : getSchema().getNumColumns();
            final ColumnData[] data = new ColumnData[numRequested];

            try (final Loader loader = new Loader()) {
                for (int i = 0; i < numRequested; i++) {
                    final int colIndex = indices != null ? indices[i] : i;
                    final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(colIndex, chunkIndex);
                    data[i] = m_cache.retainAndGet(ccUID, () -> loader.apply(ccUID), m_evictor);
                }
            }

            return data;
        }

        @Override
        public int getNumChunks() {
            if (m_numChunks == 0) {
                initDelegateReader();
                m_onUseDelegateReader.run();
                m_numChunks = m_delegateReader.getNumChunks();
            }
            return m_numChunks;
        }

        @Override
        public int getMaxDataCapacity() {
            if (m_maxDataCapacity == 0) {
                initDelegateReader();
                m_maxDataCapacity = m_delegateReader.getMaxDataCapacity();
            }
            return m_maxDataCapacity;
        }

        @Override
        public void close() throws IOException {
            if (m_delegateReader != null) {
                m_delegateReader.close();
            }
            m_readerClosed = true;
        }

    }

    private static final Object DUMMY = new Object();

    class ColumnDataUniqueId {

        private final CachedColumnReadStore m_tableStore;

        private final int m_columnIndex;

        private final int m_chunkIndex;

        ColumnDataUniqueId(final int columnIndex, final int chunkIndex) {
            m_tableStore = CachedColumnReadStore.this;
            m_columnIndex = columnIndex;
            m_chunkIndex = chunkIndex;
        }

        int getChunkIndex() {
            return m_chunkIndex;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + m_tableStore.hashCode();
            result = 31 * result + m_columnIndex;
            result = 31 * result + m_chunkIndex;
            return result;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof ColumnDataUniqueId)) {
                return false;
            }
            final ColumnDataUniqueId other = (ColumnDataUniqueId)object;
            return Objects.equals(m_tableStore, other.m_tableStore) && m_columnIndex == other.m_columnIndex
                && m_chunkIndex == other.m_chunkIndex;
        }

        @Override
        public String toString() {
            return String.join(",", m_tableStore.toString(), Integer.toString(m_columnIndex),
                Integer.toString(m_chunkIndex));
        }

    }

    private final ColumnReadStore m_delegate;

    private final LoadingEvictingCache<ColumnDataUniqueId, ColumnData> m_cache;

    private final Map<ColumnDataUniqueId, Object> m_inCache = new ConcurrentHashMap<>();

    private final BiConsumer<ColumnDataUniqueId, ColumnData> m_evictor = (k, c) -> {
        m_inCache.remove(k);
        c.release();
    };

    private int m_numChunks = 0;

    private int m_maxDataCapacity = 0;

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    /**
     * @param delegate the delegate from which to read in case of a cache miss
     * @param cache the cache for obtaining and storing data
     */
    public CachedColumnReadStore(final ColumnReadStore delegate, final CachedColumnReadStoreCache cache) {
        m_delegate = delegate;
        m_cache = cache.m_cache;
    }

    void addBatch(final ColumnData[] batch, final BiConsumer<ColumnDataUniqueId, ColumnData> evictor) {
        for (int i = 0; i < batch.length; i++) {
            m_maxDataCapacity = Math.max(m_maxDataCapacity, batch[i].getMaxCapacity());
            final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(i, m_numChunks);
            m_inCache.put(ccUID, DUMMY);
            m_cache.retainAndPut(ccUID, batch[i], evictor.andThen(m_evictor));
        }
        m_numChunks++;
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    ColumnDataReader createReader(final ColumnSelection selection, final Runnable onUseDelegateReader) {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return new CachedColumnStoreReader(selection, onUseDelegateReader);
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection selection) {
        return createReader(selection, () -> {
        });
    }

    @Override
    public void close() {
        for (final ColumnDataUniqueId id : m_inCache.keySet()) {
            final ColumnData removed = m_cache.remove(id);
            if (removed != null) {
                removed.release();
            }
        }
        m_inCache.clear();
        m_storeClosed = true;
    }
}
