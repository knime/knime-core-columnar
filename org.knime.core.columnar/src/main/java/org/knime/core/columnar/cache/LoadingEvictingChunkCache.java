package org.knime.core.columnar.cache;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.knime.core.columnar.ReferencedData;

interface LoadingEvictingChunkCache<K, C extends ReferencedData> {

	C retainAndPutIfAbsent(K key, C chunk);

	C retainAndPutIfAbsent(K key, C chunk, BiConsumer<? super K, ? super C> evictor);

	C retainAndGet(K key);

	C retainAndGet(K key, Function<? super K, ? extends C> loader, BiConsumer<? super K, ? super C> evictor);

	C remove(K key);
	
	int size();

}
