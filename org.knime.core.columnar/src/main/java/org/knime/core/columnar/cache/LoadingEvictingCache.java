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

/**
 * A cache that, similar to a {@link Map}, maps keys to values. Values are of type {@link ReferencedData}. Entries can
 * be evicted from the cache at any time. Evicted data has to be loaded back into the cache to be retrieved again. Any
 * data in the cache is retained and is only released when evicted. Data retrieved from the cache is also retained and
 * it is up to the client retrieving this data to release it.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 *
 * @param <K> the type of keys maintained by this cache
 * @param <D> the type of cached data
 */
interface LoadingEvictingCache<K, D extends ReferencedData> {

    @FunctionalInterface
    static interface Loader<D extends ReferencedData> extends Supplier<D> {
        default D loadRetained() {
            return get();
        }
    }

    @FunctionalInterface
    static interface Evictor<K, D extends ReferencedData> extends BiConsumer<K, D> {
        default void evict(final K key, final D data) {
            accept(key, data);
        }
    }

    /**
     * Associates the given data with the given key and places it in the cache.
     *
     * @param key key with which the specified data is to be associated
     * @param data data to be retained and associated with the specified key
     */
    default void put(final K key, final D data) {
        put(key, data, (k, d) -> {
        });
    }

    /**
     * Associates the given data with the given key and places it in the cache. If, at some point, the data is evicted
     * from the cache, the given evictor is called.
     *
     * @param key key with which the specified data is to be associated
     * @param data data to be retained and associated with the specified key
     * @param evictor consumer that accepts entries that are evicted
     */
    void put(final K key, final D data, final Evictor<? super K, ? super D> evictor);

    /**
     * Returns the retained data to which the specified key is mapped. Returns null if this cache contains no mapping
     * for the key. It is up to the client calling this method to release the returned data once it is no longer needed.
     *
     * @param key key whose associated value is to be returned
     * @return retained data to which the specified key is mapped, or null if this cache contains no mapping for the key
     */
    D getRetained(final K key);

    /**
     * Returns the retained data to which the specified key is mapped. If this cache contains no mapping for the key,
     * the given loader is called. The retained data provided by the loader is then associated with the given key,
     * placed in the cache, and returned. If, at some point, the data is evicted from the cache, the given evictor is
     * called. It is up to the client calling this method to release the returned data once it is no longer needed.
     *
     * @param key key whose associated value is to be returned
     * @param loader supplier of retained data
     * @param evictor consumer that accepts entries that are evicted
     * @return retained, already present or recently loaded data to which the specified key is mapped, or null if the
     *         loader returned null
     */
    D getRetained(final K key, final Loader<? extends D> loader, final Evictor<? super K, ? super D> evictor);

    /**
     * Removes the mapping for a key from this cache if it is present (optional operation). Returns the retained data to
     * which this cache previously associated the key, or <tt>null</tt> if the cache contained no mapping for the key.
     * It is up to the client calling this method to release the returned data once it is no longer needed.
     *
     * @param key key whose mapping is to be removed from the cache
     * @return the retained data to which the specified key was mapped, or null if the cache contained no mapping for
     *         the key
     */
    D removeRetained(final K key);

    /**
     * Returns the approximate number of key-data mappings in this cache.
     *
     * @return the approximate number of key-data mappings in this cache
     */
    int size();
}
