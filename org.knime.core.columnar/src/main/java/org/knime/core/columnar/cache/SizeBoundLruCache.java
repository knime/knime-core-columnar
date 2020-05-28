package org.knime.core.columnar.cache;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.knime.core.columnar.ReferencedData;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * @author "Marc Bux, KNIME GmbH, Berlin, Germany"
 *
 * @param <K>
 * @param <C>
 */
final class SizeBoundLruCache<K, C extends ReferencedData> implements LoadingEvictingChunkCache<K, C> {
	
	private final class ChunkWithEvictor {
		private final C m_chunk;
		private final BiConsumer<? super K, ? super C> m_evictor;

		ChunkWithEvictor(final C chunk, BiConsumer<? super K, ? super C> evictor) {
			m_chunk = chunk;
			m_evictor = evictor;
		}
	}

	private final Map<K, ChunkWithEvictor> m_lruCache;

	// TODO: we have to be wary of integer overflows here; ideally, we would switch
	// to longs, but the weigher does not support it; a solution might be to round
	// up to kilobytes
	SizeBoundLruCache(final int maxSize) {
		m_lruCache = (new ConcurrentLinkedHashMap.Builder<K, ChunkWithEvictor>()) //
				.maximumWeightedCapacity(maxSize) //
				// TODO: How should we deal with empty chunks? The weigher (reasonably) requires a weight of at least 1!
				.weigher(chunkWithEvictor -> (int) Math.max(1, chunkWithEvictor.m_chunk.sizeOf())) // cache is bound by size in bytes
				.listener((k, evicted) -> {
					evicted.m_evictor.accept(k, evicted.m_chunk);
					evicted.m_chunk.release();
				}) // on eviction, run evictor and release chunk
				.build();
	}
	
	@Override
	public C retainAndPutIfAbsent(K key, C chunk) {
		return retainAndPutIfAbsent(key, chunk, (k, c) -> {
		});
	}

	@Override
	public C retainAndPutIfAbsent(K key, C chunk, BiConsumer<? super K, ? super C> evictor) {
		ChunkWithEvictor ce = m_lruCache.computeIfAbsent(key, k -> {
			chunk.retain();
			return new ChunkWithEvictor(chunk, evictor);
		});
		
		return ce.m_chunk;
	}

	@Override
	public C retainAndGet(K key) {
		final ChunkWithEvictor cached = m_lruCache.computeIfPresent(key, (k, c) -> {
			c.m_chunk.retain(); // retain for the caller of this method
			return c;
		});
		return cached == null ? null : cached.m_chunk;
	}

	// TODO this could be problematic for two reasons:
	// (1) it might take a long time loading a new ColumnChunk, especially if we are
	// waiting for a flush
	// (2) we replace already existing entries with themselves, leading to another
	// weighing, amongst other things
	@Override
	public C retainAndGet(K key, Function<? super K, ? extends C> loader, BiConsumer<? super K, ? super C> evictor) {
		return m_lruCache.compute(key, (k, c) -> {
			if (c == null) {
				final C loaded = loader.apply(k);
				assert loaded != null;
				assert loaded.sizeOf() > 0;
				loaded.retain(); // retain for the cache (just as if we retainAndPutifAbsent were called)
				loaded.retain(); // retain for the caller of this method
				return new ChunkWithEvictor(loaded, evictor);
			}
			c.m_chunk.retain(); // retain for the caller of this method
			return c;
		}).m_chunk;
	}

	@Override
	public C remove(K key) {
		final ChunkWithEvictor removed = m_lruCache.remove(key);
		if (removed != null) {
			return removed.m_chunk;
		}
		return null;
	}
	
	@Override
	public int size() {
		return m_lruCache.size();
	}

}
