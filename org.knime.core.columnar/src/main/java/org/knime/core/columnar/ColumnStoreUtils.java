package org.knime.core.columnar;

import org.knime.core.columnar.cache.AsyncFlushColumnStore;
import org.knime.core.columnar.cache.CachedColumnReadStore;
import org.knime.core.columnar.cache.CachedColumnStore;
import org.knime.core.columnar.cache.SmallColumnStore;

public class ColumnStoreUtils {

	private static final int SMALL_TABLE_THRESHOLD = 1 << 20; // 1 MB
	private static final int SMALL_TABLE_CACHE_SIZE = 1 << 25; // 32 MB, i.e., holds up to 32 small tables
	private static final int COLUMN_CHUNK_CACHE_SIZE = 1 << 30; // 1 GB

	public static ColumnStore cache(final ColumnStore store) {
		return new SmallColumnStore(new CachedColumnStore(new AsyncFlushColumnStore(store), COLUMN_CHUNK_CACHE_SIZE),
				+SMALL_TABLE_THRESHOLD, SMALL_TABLE_CACHE_SIZE);
	}

	public static ColumnReadStore cache(final ColumnReadStore store) {
		return new CachedColumnReadStore(store, 1 << 30);
	}

}
