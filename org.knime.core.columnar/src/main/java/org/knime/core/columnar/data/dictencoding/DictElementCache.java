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

import java.util.HashMap;
import java.util.Map;

import org.knime.core.columnar.data.dictencoding.DictKeys.DictKeyGenerator;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

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

        /**
         * Create a {@link ColumnDictElementCache} using the given dictionary key type
         *
         * @param keyType The dictionary {@link KeyType} used for this column
         */
        public ColumnDictElementCache(final KeyType keyType) {
            m_dictKeyGenerator = DictKeys.createAscendingKeyGenerator(keyType);
        }

        @Override
        public synchronized <T> K generateKey(final T value) {
            // synchronized because that could possibly be called from multiple threads?
            return m_dictKeyGenerator.generateKey(value);
        }
    }

    private Map<Integer, ColumnDictElementCache<Byte>> m_perColumnCacheWithByteKeys = new HashMap<>();

    private Map<Integer, ColumnDictElementCache<Integer>> m_perColumnCacheWithIntKeys = new HashMap<>();

    private Map<Integer, ColumnDictElementCache<Long>> m_perColumnCacheWithLongKeys = new HashMap<>();

    /**
     * Get the cache for a column by column index
     *
     * @param <K> The type used for dictionary keys at the specified column
     * @param columnIndex The column for which to get the cache.
     * @param keyType The {@link KeyType} used for the dictionary at the requested column
     * @return The cache for that column. Lazily created if this is the first access.
     */
    @SuppressWarnings("unchecked")
    public <K> ColumnDictElementCache<K> get(final int columnIndex, final KeyType keyType) {
        if (keyType == KeyType.BYTE_KEY) {
            return (ColumnDictElementCache<K>)m_perColumnCacheWithByteKeys.computeIfAbsent(columnIndex,
                i -> new ColumnDictElementCache<Byte>(keyType));
        } else if (keyType == KeyType.INT_KEY) {
            return (ColumnDictElementCache<K>)m_perColumnCacheWithIntKeys.computeIfAbsent(columnIndex,
                i -> new ColumnDictElementCache<Integer>(keyType));
        } else if (keyType == KeyType.LONG_KEY) {
            return (ColumnDictElementCache<K>)m_perColumnCacheWithLongKeys.computeIfAbsent(columnIndex,
                i -> new ColumnDictElementCache<Long>(keyType));
        } else {
            throw new UnsupportedOperationException("Cannot create table-wide dictionary cache for " + keyType);
        }
    }
}
