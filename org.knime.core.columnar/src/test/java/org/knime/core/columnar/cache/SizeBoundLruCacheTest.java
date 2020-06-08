package org.knime.core.columnar.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.knime.core.columnar.cache.CacheTestUtils.TestColumnData;

public class SizeBoundLruCacheTest {

	@Test
	public void testPutGet() throws Exception {
		final LoadingEvictingCache<Integer, TestColumnData> cache = new SizeBoundLruCache<>(1);
		final TestColumnData data = new TestColumnData(1);
		assertEquals(0, data.getRefs());

		cache.retainAndPutIfAbsent(1, data);
		assertEquals(1, cache.size());
		assertEquals(1, data.getRefs());

		assertEquals(data, cache.retainAndGet(1));
		assertEquals(2, data.getRefs());

		assertEquals(data, cache.retainAndGet(1, () -> null, (i, d) -> {
		}));
		assertEquals(3, data.getRefs());
	}

	@Test
	public void testPutEvictLoadGet() throws Exception {
		final LoadingEvictingCache<Integer, TestColumnData> cache = new SizeBoundLruCache<>(1);
		final TestColumnData data1 = new TestColumnData(1);
		final TestColumnData data2 = new TestColumnData(1);
		assertEquals(0, data1.getRefs());

		final AtomicBoolean evicted = new AtomicBoolean();
		cache.retainAndPutIfAbsent(1, data1, (i, d) -> evicted.set(true));
		assertEquals(1, cache.size());
		assertEquals(1, data1.getRefs());

		cache.retainAndPutIfAbsent(2, data2);
		assertEquals(true, evicted.get());
		assertEquals(1, cache.size());
		assertEquals(0, data1.getRefs());

		assertNull(cache.retainAndGet(1));
		assertEquals(data1, cache.retainAndGet(1, () -> {
			data1.retain();
			return data1;
		}, (i, data) -> {
		}));
		assertEquals(2, data1.getRefs());
	}
	
	@Test
	public void testPutRemove() throws Exception {
		final LoadingEvictingCache<Integer, TestColumnData> cache = new SizeBoundLruCache<>(1);
		final TestColumnData data = new TestColumnData(1);
		assertEquals(0, data.getRefs());

		cache.retainAndPutIfAbsent(1, data);
		assertEquals(1, cache.size());
		assertEquals(1, data.getRefs());

		assertEquals(data, cache.remove(1));
		assertEquals(1, data.getRefs());
		
		assertNull(cache.remove(1));
	}
	
	@Test
	public void testLru() throws Exception {
		final LoadingEvictingCache<Integer, TestColumnData> cache = new SizeBoundLruCache<>(2);
		final TestColumnData data1 = new TestColumnData(1);
		final TestColumnData data2 = new TestColumnData(1);
		final TestColumnData data3 = new TestColumnData(1);
		
		cache.retainAndPutIfAbsent(1, data1); // content in cache: 1
		cache.retainAndPutIfAbsent(2, data2); // content in cache: 2->1
		cache.retainAndPutIfAbsent(3, data3); // content in cache: 3->2
		assertEquals(2, cache.size());
		
		assertEquals(data3, cache.retainAndGet(3)); // content in cache: 3->2
		assertEquals(data2, cache.retainAndGet(2)); // content in cache: 2->3
		assertNull(cache.retainAndGet(1));
		
		cache.retainAndPutIfAbsent(1, data1); // content in cache: 1->2
		assertEquals(data1, cache.retainAndGet(1));
		assertEquals(data2, cache.retainAndGet(2));
		assertNull(cache.retainAndGet(3));
	}

}
