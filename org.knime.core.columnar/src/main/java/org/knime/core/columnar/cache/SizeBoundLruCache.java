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
 * A {@link LoadingEvictingCache} that holds data up to a fixed maximum
 * {@link ReferencedData#sizeOf() size} in bytes. Once the size threshold is
 * reached, data is evicted from the cache in least-recently-used manner.
 * 
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 *
 * @param <K> the type of keys maintained by this cache
 * @param <D> the type of cached data
 */
final class SizeBoundLruCache<K, D extends ReferencedData> implements LoadingEvictingCache<K, D> {

	private final class DataWithEvictor {
		private final D m_data;
		private final BiConsumer<? super K, ? super D> m_evictor;

		DataWithEvictor(final D data, BiConsumer<? super K, ? super D> evictor) {
			m_data = data;
			m_evictor = evictor;
		}
	}

	private final Map<K, DataWithEvictor> m_lruCache;

	SizeBoundLruCache(final long maxSize) {
		
		final Weigher<K, DataWithEvictor> weigher = (k, dataWithEvictor) -> Math.max(1, dataWithEvictor.m_data.sizeOf());
		
		final RemovalListener<K, DataWithEvictor> removalListener = removalNotification -> {
			if (removalNotification.wasEvicted()) {
				final K k = removalNotification.getKey();
				final DataWithEvictor evicted = removalNotification.getValue();
				evicted.m_evictor.accept(k, evicted.m_data);
				evicted.m_data.release();
			}
		};
		
		final Cache<K, DataWithEvictor> cache = CacheBuilder.newBuilder().maximumWeight(maxSize).weigher(weigher)
				.removalListener(removalListener).build();
				
		m_lruCache = cache.asMap();
	}

	@Override
	public void retainAndPutIfAbsent(K key, D data, BiConsumer<? super K, ? super D> evictor) {
		m_lruCache.computeIfAbsent(key, k -> {
			data.retain();
			return new DataWithEvictor(data, evictor);
		});
	}

	@Override
	public D retainAndGet(K key) {
		final DataWithEvictor cached = m_lruCache.computeIfPresent(key, (k, c) -> {
			c.m_data.retain(); // retain for the caller of this method
			return c;
		});
		return cached == null ? null : cached.m_data;
	}

	@Override
	public D retainAndGet(K key, Supplier<? extends D> loader, BiConsumer<? super K, ? super D> evictor) {
		return m_lruCache.compute(key, (k, c) -> {
			if (c == null) {
				final D loaded = loader.get(); // data is already retained by the loader
				loaded.retain(); // retain for the caller of this method
				return new DataWithEvictor(loaded, evictor);
			}
			c.m_data.retain(); // retain for the caller of this method
			return c;
		}).m_data;
	}

	@Override
	public D remove(K key) {
		final DataWithEvictor removed = m_lruCache.remove(key);
		if (removed != null) {
			return removed.m_data;
		}
		return null;
	}

	@Override
	public int size() {
		return m_lruCache.size();
	}
}
