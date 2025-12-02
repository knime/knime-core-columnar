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
 *   2 Sep 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.cache.data;

import java.util.function.Predicate;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.memory.ColumnarOffHeapMemoryAlertSystem;
import org.knime.core.columnar.store.UseOnHeapColumnStoreProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache for storing data that can be shared between multiple {@link ReadDataCache ReadDataCaches} and
 * {@link ReadDataReadCache ReadDataReadCaches}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class SharedReadDataCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(SharedReadDataCache.class);

    private final Cache<ColumnDataUniqueId, NullableReadData> m_cache;

    private final long m_cacheSizeBytes;

    /**
     * @param cacheSizeBytes the maximum size of the cache in bytes
     * @param concurrencyLevel the allowed concurrency among update operations
     */
    public SharedReadDataCache(final long cacheSizeBytes, final int concurrencyLevel) {

        final Weigher<ColumnDataUniqueId, NullableReadData> weigher = (k, v) -> {
            final long size = v.sizeOf();
            if (size > Integer.MAX_VALUE) {
                final Logger logger = LoggerFactory.getLogger(getClass());
                logger.error("Size of data ({}) is larger than maximum ({}).", size, Integer.MAX_VALUE);
                return Integer.MAX_VALUE;
            }
            return Math.max(1, (int)size);
        };

        final RemovalListener<ColumnDataUniqueId, NullableReadData> removalListener = removalNotification -> {
            final NullableReadData data = removalNotification.getValue();
            if (data != null) {
                data.release();
            }
        };

        m_cache = CacheBuilder.newBuilder() //
                .concurrencyLevel(concurrencyLevel) //
                .maximumWeight(cacheSizeBytes) //
                .weigher(weigher) //
                .removalListener(removalListener) //
                .build();

        m_cacheSizeBytes = cacheSizeBytes;

        if (!UseOnHeapColumnStoreProperty.useOnHeapColumnStore()) {
            ColumnarOffHeapMemoryAlertSystem.INSTANCE.addMemoryListener(this::clear);
        }
    }

    /**
     * Get the maximum size of this cache in bytes.
     *
     * @return maximum size of cache in bytes.
     */
    public long getMaxSizeInBytes() {
        return m_cacheSizeBytes;
    }

    /**
     * Clears the cache by invalidating all entries.
     *
     * @return <code>true</code> if the cache was cleared, <code>false</code> if it was already empty.
     */
    public boolean clear() {
        var numEntries = m_cache.size();
        if (numEntries > 0) {
            LOGGER.info("Received memory alert. Clearing approximately {} entries.", numEntries);
            m_cache.invalidateAll();
            m_cache.cleanUp();
            return true;
        }
        return false;
    }

    /**
     * Returns the approximate number of key-data mappings in this cache.
     *
     * @return the approximate number of key-data mappings in this cache
     */
    int size() {
        return m_cache.asMap().size();
    }

    /**
     * Associates the given data with the given key and places it in the cache.
     * <p>
     * The given {@code value} is {@link ReferencedData#retain() retained} for the cache.
     * <p>
     * If another value was already associated with the same key, the cache will {@link ReferencedData#release()
     * release} it.
     *
     * @param key key with which the specified data is to be associated
     * @param value data to be retained and associated with the specified key
     */
    void put(final ColumnDataUniqueId key, final NullableReadData value) {
        value.retain();
        m_cache.put(key, value);
    }

    /**
     * Retain and return the value associated with the specified {@code key}. Returns {@code null} if this cache
     * contains no mapping for the key.
     * <p>
     * The returned data is {@link ReferencedData#retain() retained} for the caller. It is up to the caller to
     * {@link ReferencedData#release() release} the returned data once it is no longer needed.
     *
     * @param key the key
     * @return retained data to which the specified key is mapped, or null if this cache contains no mapping for the key
     */
    NullableReadData getRetained(final ColumnDataUniqueId key) {
        final NullableReadData cached = m_cache.getIfPresent(key);
        if (cached != null) {
            try {
                cached.retain();
                return cached;
            } catch (IllegalStateException e) { // NOSONAR
                // we should only end up here in the very rare case where the data is evicted in between get() and retain()
            }
        }
        return null;
    }

    /**
     * Removes all mappings with keys satisfying the given predicate. Removed values are {@link ReferencedData#release()
     * released}.
     *
     * @param predicate a predicate which returns {@code true} for keys to be removed
     */
    void invalidateIf(final Predicate<? super ColumnDataUniqueId> predicate) {
        m_cache.asMap().keySet().removeIf(predicate);
        m_cache.cleanUp();
    }
}
