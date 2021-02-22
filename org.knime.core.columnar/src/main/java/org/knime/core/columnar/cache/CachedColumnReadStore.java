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

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.LoadingEvictingCache.Evictor;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.DelegatingColumnReadStore;

/**
 * A {@link ColumnReadStore} that reads {@link NullableWriteData} from a delegate column store and places it in a
 * fixed-size {@link SmallColumnStoreCache LRU cache} in memory for faster subsequent access. The store allows
 * concurrent reading via multiple {@link BatchReader ColumnDataReaders}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class CachedColumnReadStore extends DelegatingColumnReadStore {

    private final class CachedBatchReader extends DelegatingBatchReader {

        private final BufferedBatchLoader m_batchLoader;

        private final Evictor<ColumnDataUniqueId, NullableReadData> m_evictor = (k, c) -> m_cachedData.remove(k);

        CachedBatchReader(final ColumnSelection selection) {
            super(CachedColumnReadStore.this, selection);
            m_batchLoader = new BufferedBatchLoader();
        }

        @Override
        protected ReadBatch readRetainedInternal(final int index) throws IOException {
            return getSelection().createBatch(i -> { // NOSONAR
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(CachedColumnReadStore.this, i, index);
                return m_globalCache.getRetained(ccUID, () -> { // NOSONAR
                    m_cachedData.add(ccUID);
                    try {
                        @SuppressWarnings("resource")
                        final NullableReadData data = m_batchLoader.loadBatch(initAndGetDelegate(), index).get(i);
                        data.retain();
                        return data;
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }, m_evictor);
            });
        }

        @Override
        protected void closeOnce() throws IOException {
            m_batchLoader.close();
            super.closeOnce();
        }

    }

    private final LoadingEvictingCache<ColumnDataUniqueId, NullableReadData> m_globalCache;

    private Set<ColumnDataUniqueId> m_cachedData = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * @param delegate the delegate from which to read in case of a cache miss
     * @param cache the cache for obtaining and storing data
     */
    public CachedColumnReadStore(final ColumnReadStore delegate, final CachedColumnStoreCache cache) {
        super(delegate);
        m_globalCache = cache.getCache();
    }

    @Override
    protected BatchReader createReaderInternal(final ColumnSelection selection) {
        return new CachedBatchReader(selection);
    }

    @Override
    protected void closeOnce() throws IOException {
        for (final ColumnDataUniqueId id : m_cachedData) {
            final NullableReadData removed = m_globalCache.removeRetained(id);
            if (removed != null) {
                removed.release();
            }
        }
        m_cachedData.clear();
        m_cachedData = null;
        super.closeOnce();
    }

}
