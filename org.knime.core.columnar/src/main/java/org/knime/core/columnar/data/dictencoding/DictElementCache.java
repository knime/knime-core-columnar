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
 *   Aug 12, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data.dictencoding;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.knime.core.columnar.data.dictencoding.DictKeys.DictKeyGenerator;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * This {@link DictElementCache} will be used to cache entries of dictionary-encoded data across all batches of the
 * table, or at least as long as they do fit in memory.
 *
 * TODO: implement the cache in AP-17248 and register it with the MemoryAlertListener
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DictElementCache {

    /**
     * The cache for the entries of one column in the table.
     *
     * Supports the generation of globally unique dictionary keys.
     *
     * @param <K> type of dictionary keys
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class ColumnDictElementCache<K> implements DictKeyGenerator<K> {
        private final DictKeyGenerator<K> m_dictKeyGenerator;

        // We use weak cache-keys because the cache stores the mapping of dictEntry->dictKey
        // and the dictEntries are potentially large.
        private final Cache<?, K> m_cache = CacheBuilder.newBuilder().weakKeys().build();

        private final Map<?, K> m_dictKeyCache;

        /**
         * Create a {@link ColumnDictElementCache} using the given dictionary key type
         *
         * @param keyType The dictionary {@link KeyType} used for this column
         */
        public ColumnDictElementCache(final KeyType keyType) {
            m_dictKeyGenerator = DictKeys.createAscendingKeyGenerator(keyType);
            m_dictKeyCache = m_cache.asMap();
        }

        @SuppressWarnings("unchecked")
        @Override
        public synchronized <T> K generateKey(final T value) {
            // synchronized because that could possibly be called from multiple threads?
            return ((Map<T, K>)m_dictKeyCache).computeIfAbsent(value, m_dictKeyGenerator::generateKey);
        }

        synchronized void clearCache() {
            m_cache.invalidateAll();
        }
    }

    private Map<DataIndex, ColumnDictElementCache<Byte>> m_perColumnCacheWithByteKeys = new ConcurrentHashMap<>();

    private Map<DataIndex, ColumnDictElementCache<Integer>> m_perColumnCacheWithIntKeys = new ConcurrentHashMap<>();

    private Map<DataIndex, ColumnDictElementCache<Long>> m_perColumnCacheWithLongKeys = new ConcurrentHashMap<>();

    /**
     * Get the cache for a column by column index
     *
     * @param <K> The type used for dictionary keys at the specified column
     * @param dataIndex The data for which to get the cache
     * @param keyType The {@link KeyType} used for the dictionary at the requested column
     * @return The cache for that column. Lazily created if this is the first access.
     */
    @SuppressWarnings("unchecked")
    public <K> ColumnDictElementCache<K> get(final DataIndex dataIndex, final KeyType keyType) {
        if (keyType == KeyType.BYTE_KEY) {
            return (ColumnDictElementCache<K>)m_perColumnCacheWithByteKeys.computeIfAbsent(dataIndex,
                i -> new ColumnDictElementCache<Byte>(keyType));
        } else if (keyType == KeyType.INT_KEY) {
            return (ColumnDictElementCache<K>)m_perColumnCacheWithIntKeys.computeIfAbsent(dataIndex,
                i -> new ColumnDictElementCache<Integer>(keyType));
        } else if (keyType == KeyType.LONG_KEY) {
            return (ColumnDictElementCache<K>)m_perColumnCacheWithLongKeys.computeIfAbsent(dataIndex,
                i -> new ColumnDictElementCache<Long>(keyType));
        } else {
            throw new UnsupportedOperationException("Cannot create table-wide dictionary cache for " + keyType);
        }
    }

    /**
     * Clear the caches, e.g. due to memory pressure
     */
    public synchronized void clearCaches() {
        for (var cache : m_perColumnCacheWithByteKeys.values()) {
            cache.clearCache();
        }

        for (var cache : m_perColumnCacheWithIntKeys.values()) {
            cache.clearCache();
        }

        for (var cache : m_perColumnCacheWithLongKeys.values()) {
            cache.clearCache();
        }
    }

    /**
     * Identifies a particular data object within a nested data structure.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class DataIndex {

        private final DataIndex m_parent;

        private final int m_index;

        private final int m_hashCode;

        private DataIndex(final int columnIndex) {
            m_parent = null;
            m_index = columnIndex;
            m_hashCode = columnIndex;
        }

        private DataIndex(final DataIndex parent, final int childIndex) {
            m_parent = parent;
            m_index = childIndex;
            m_hashCode = m_parent.hashCode() + 37 * childIndex;
        }

        /**
         * Creates the root index for a column.
         *
         * @param columnIndex index of the column within the table
         * @return an index identifying the root level of the column
         */
        public static DataIndex createColumnIndex(final int columnIndex) {
            return new DataIndex(columnIndex);
        }

        /**
         * Creates a child index.
         *
         * @param childIndex index of the child
         * @return an index identifying the child at the provided index
         */
        public DataIndex createChild(final int childIndex) {
            return new DataIndex(this, childIndex);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            } else if (obj instanceof DataIndex) {
                var other = (DataIndex)obj;
                return m_hashCode == other.m_hashCode//
                    && m_index == other.m_index//
                    && Objects.equals(m_parent, other.m_parent);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return m_hashCode;
        }

    }
}
