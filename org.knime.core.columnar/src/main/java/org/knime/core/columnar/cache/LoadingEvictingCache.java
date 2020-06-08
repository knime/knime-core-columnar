package org.knime.core.columnar.cache;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.knime.core.columnar.ReferencedData;

/**
 * A thread-safe cache that, similar to a {@link Map}, maps keys to values.
 * Values are of type {@link ReferencedData}. Entries can be evicted from the
 * cache at any time. Evicted data has to be loaded back into the cache to be
 * retrieved again. Any data in the cache is retained and is only released when
 * evicted. Data retrieved from the cache is also retained and it is up to the
 * client retrieving this data to release it.
 * 
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 *
 * @param <K> the type of keys maintained by this cache
 * @param <D> the type of cached data
 */
interface LoadingEvictingCache<K, D extends ReferencedData> {

	/**
	 * If the specified key is not already associated with data, retains the given
	 * data and associates it with the key.
	 * 
	 * @param key  key with which the specified data is to be associated
	 * @param data data to be retained and associated with the specified key
	 */
	default void retainAndPutIfAbsent(K key, D data) {
		retainAndPutIfAbsent(key, data, (k, d) -> {
		});
	}

	/**
	 * If the specified key is not already associated with data, retains the given
	 * data and associates it with the key. If, at some point, the data is evicted
	 * from the cache (and about to be released), the given evictor is called. The
	 * evictor must not release the data itself.
	 * 
	 * @param key     key with which the specified data is to be associated
	 * @param data    data to be retained and associated with the specified key
	 * @param evictor consumer that accepts entries that are evicted and about to be
	 *                released
	 */
	void retainAndPutIfAbsent(K key, D data, BiConsumer<? super K, ? super D> evictor);

	/**
	 * Retains the data to which the specified key is mapped and returns it. Returns
	 * null if this cache contains no mapping for the key. It is up to the client
	 * calling this method to release the returned data once it is no longer needed.
	 * 
	 * @param key key whose associated value is to be returned
	 * @return retained data to which the specified key is mapped, or null if this
	 *         cache contains no mapping for the key
	 */
	D retainAndGet(K key);

	/**
	 * Retains the data to which the specified key is mapped and returns it. If this
	 * cache contains no mapping for the key, the given loader is called. The
	 * retained, non-null, data provided by the loader is then associated with the
	 * given key, retained (again), and returned. If, at some point, the data is
	 * evicted from the cache (and about to be released), the given evictor is
	 * called. The evictor must not release the data itself. It is up to the client
	 * calling this method to release the returned data once it is no longer needed.
	 * 
	 * @param key     key whose associated value is to be returned
	 * @param loader  supplier of retained, data
	 * @param evictor consumer that accepts entries that are evicted and about to be
	 *                released
	 * @return retained, already present or recently loaded data to which the
	 *         specified key is mapped
	 */
	D retainAndGet(K key, Supplier<? extends D> loader, BiConsumer<? super K, ? super D> evictor);

	/**
	 * Removes the mapping for a key from this cache if it is present (optional
	 * operation). Returns the retained data to which this cache previously
	 * associated the key, or <tt>null</tt> if the cache contained no mapping for
	 * the key. It is up to the client calling this method to release the returned
	 * data once it is no longer needed.
	 * 
	 * @param key key whose mapping is to be removed from the cache
	 * @return the retained data to which the specified key was mapped, or null if
	 *         the cache contained no mapping for the key
	 */
	D remove(K key);

	/**
	 * Returns the number of key-data mappings in this cache.
	 *
	 * @return the number of key-data mappings in this cache
	 */
	int size();
}
