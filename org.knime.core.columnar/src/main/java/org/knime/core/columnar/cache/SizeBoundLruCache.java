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

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.knime.core.columnar.ReferencedData;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;

/**
 * A thread-safe LoadingEvictingCache that holds data up to a fixed maximum {@link ReferencedData#sizeOf() size} in
 * bytes. Once the size threshold is reached, data is evicted from the cache in least-recently-used manner.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 *
 * @param <K> the type of keys maintained by this cache
 * @param <D> the type of cached data
 */
final class SizeBoundLruCache<K, D extends ReferencedData> implements LoadingEvictingCache<K, D> {

    private static final class DataWithEvictor<K, D extends ReferencedData> {
        private final D m_data;

        private final BiConsumer<? super K, ? super D> m_evictor;

        DataWithEvictor(final D data, final BiConsumer<? super K, ? super D> evictor) {
            m_data = data;
            m_evictor = evictor;
        }
    }

    private final Map<K, DataWithEvictor<K, D>> m_lruCache;

    SizeBoundLruCache(final long maxSize) {

        final Weigher<K, DataWithEvictor<K, D>> weigher =
            (k, dataWithEvictor) -> Math.max(1, dataWithEvictor.m_data.sizeOf());

        final RemovalListener<K, DataWithEvictor<K, D>> removalListener = removalNotification -> {
            if (removalNotification.wasEvicted()) {
                final DataWithEvictor<K, D> evicted = removalNotification.getValue();
                evicted.m_evictor.accept(removalNotification.getKey(), evicted.m_data);
            }
        };

        final Cache<K, DataWithEvictor<K, D>> cache =
            CacheBuilder.newBuilder().maximumWeight(maxSize).weigher(weigher).removalListener(removalListener).build();

        m_lruCache = cache.asMap();
    }

    @Override
    public void retainAndPut(final K key, final D data, final BiConsumer<? super K, ? super D> evictor) {
        data.retain();
        m_lruCache.put(key, new DataWithEvictor<K, D>(data, evictor));
    }

    @Override
    public D retainAndGet(final K key) {
        final DataWithEvictor<K, D> cached = m_lruCache.computeIfPresent(key, (k, c) -> {
            c.m_data.retain(); // retain for the caller of this method
            return c;
        });
        return cached == null ? null : cached.m_data;
    }

    @Override
    public D retainAndGet(final K key, final Supplier<? extends D> loader,
        final BiConsumer<? super K, ? super D> evictor) {
        return m_lruCache.compute(key, (k, c) -> {
            if (c == null) {
                final D loaded = loader.get(); // data is already retained by the loader
                loaded.retain(); // retain for the caller of this method
                return new DataWithEvictor<K, D>(loaded, evictor);
            } else {
                c.m_data.retain(); // retain for the caller of this method
                return c;
            }
        }).m_data;
    }

    @Override
    public D remove(final K key) {
        final DataWithEvictor<K, D> removed = m_lruCache.remove(key);
        return removed == null ? null : removed.m_data;
    }

    @Override
    public int size() {
        return m_lruCache.size();
    }

}
