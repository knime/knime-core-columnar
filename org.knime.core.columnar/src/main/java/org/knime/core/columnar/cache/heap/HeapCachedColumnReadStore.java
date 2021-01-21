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
 * History
 *   Oct 9, 2020 (dietzc): created
 */
package org.knime.core.columnar.cache.heap;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.ObjectData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.DelegatingColumnReadStore;

/**
 * {@link ColumnReadStore} for in-heap caching of {@link ObjectData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @since 4.3
 */
public final class HeapCachedColumnReadStore extends DelegatingColumnReadStore {

    private final class Reader extends DelegatingBatchReader {

        private Reader(final ColumnSelection selection) {
            super(HeapCachedColumnReadStore.this, selection);
        }

        @Override
        protected ReadBatch readRetainedInternal(final int index) throws IOException {
            final ReadBatch batch = super.readRetainedInternal(index);
            return getSelection().createBatch(i -> m_objectData.isSelected(i) ? wrap(batch, index, i) : batch.get(i));
        }

        @SuppressWarnings("unchecked")
        private <T> HeapCachedLoadingReadData<T> wrap(final ReadBatch batch, final int batchIndex,
            final int columnIndex) {
            final ObjectReadData<T> columnReadData = (ObjectReadData<T>)batch.get(columnIndex);
            final Object[] array = m_cache
                .computeIfAbsent(new ColumnDataUniqueId(HeapCachedColumnReadStore.this, columnIndex, batchIndex), k -> {
                    m_cachedData.add(k);
                    return new Object[columnReadData.length()];
                });
            return new HeapCachedLoadingReadData<>(columnReadData, array);
        }

    }

    private final ColumnSelection m_objectData;

    private final Map<ColumnDataUniqueId, Object[]> m_cache;

    private Set<ColumnDataUniqueId> m_cachedData;

    /**
     * @param delegate the delegate from which to read
     * @param cache the delegate from which to read object data in case of a cache miss
     */
    public HeapCachedColumnReadStore(final ColumnReadStore delegate, final ObjectDataCache cache) {
        this(delegate, HeapCacheUtils.getObjectDataIndices(delegate.getSchema()), cache,
            Collections.newSetFromMap(new ConcurrentHashMap<>()));
    }

    HeapCachedColumnReadStore(final ColumnReadStore delegate, final ObjectDataCache cache,
        final Set<ColumnDataUniqueId> cachedData) {
        this(delegate, HeapCacheUtils.getObjectDataIndices(delegate.getSchema()), cache, cachedData);
    }

    HeapCachedColumnReadStore(final ColumnReadStore delegate, final ColumnSelection selection,
        final ObjectDataCache cache, final Set<ColumnDataUniqueId> cachedData) {
        super(delegate);
        m_objectData = selection;
        m_cache = cache.getCache();
        m_cachedData = cachedData;
    }

    @Override
    protected BatchReader createReaderInternal(final ColumnSelection selection) {
        return new Reader(selection);
    }

    @Override
    protected void closeOnce() throws IOException {
        m_cache.keySet().removeAll(m_cachedData);
        m_cachedData.clear();
        m_cachedData = null;
        super.closeOnce();
    }

    void onEviction(final ColumnDataUniqueId ccuid) {
        m_cachedData.remove(ccuid);
    }

}
