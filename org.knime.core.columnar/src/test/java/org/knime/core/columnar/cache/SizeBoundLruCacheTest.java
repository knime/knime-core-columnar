package org.knime.core.columnar.cache;

import org.junit.Test;
import org.junit.Assert;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.cache.SizeBoundLruCache;

public class SizeBoundLruCacheTest {

	private static class TestData implements ReferencedData {

		private final int m_sizeOf;

		private int m_refs;

		public TestData(int sizeOf) {
			m_sizeOf = sizeOf;
		}

		@Override
		public void release() {
			m_refs--;
		}

		@Override
		public void retain() {
			m_refs++;
		}

		@Override
		public int sizeOf() {
			return m_sizeOf;
		}
	}

	@Test
	public void sizeTest() {
		SizeBoundLruCache<Integer, TestData> cache = new SizeBoundLruCache<>(5);
		TestData c1 = new TestData(2);
		TestData c2 = new TestData(2);
		TestData c3 = new TestData(2);

		cache.retainAndPutIfAbsent(1, c1);
		cache.retainAndPutIfAbsent(2, c2);
		cache.retainAndPutIfAbsent(3, c3);

		Assert.assertEquals(null, cache.retainAndGet(1));
		Assert.assertEquals(c2, cache.retainAndGet(2));
		c2.release();
		Assert.assertEquals(c3, cache.retainAndGet(3));
		c3.release();
		cache.remove(2);
		c2.release();
		cache.remove(3);
		c3.release();
		Assert.assertEquals(0, c1.m_refs);
		Assert.assertEquals(0, c2.m_refs);
		Assert.assertEquals(0, c3.m_refs);
	}

}
