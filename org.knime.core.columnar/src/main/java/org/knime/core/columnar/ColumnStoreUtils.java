package org.knime.core.columnar;

import org.knime.core.columnar.cache.AsyncFlushColumnStore;
import org.knime.core.columnar.cache.AsyncFlushColumnStore.AsyncFlushColumnStoreExecutor;
import org.knime.core.columnar.cache.CachedColumnReadStore;
import org.knime.core.columnar.cache.CachedColumnReadStore.CachedColumnReadStoreCache;
import org.knime.core.columnar.cache.CachedColumnStore;
import org.knime.core.columnar.cache.SmallColumnStore;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;

public class ColumnStoreUtils {

	public static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

	public static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

	public static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

	// the size (in bytes) up to which a table is considered small
	private static final int SMALL_TABLE_THRESHOLD = 1 << 20; // 1 MB

	// the size (in bytes) of the LRU cache for entire small tables
	private static final long SMALL_TABLES_CACHE_SIZE = 1L << 25; // 32 MB, i.e., holds up to 32 small tables

	private static final SmallColumnStoreCache SMALL_TABLES_CACHE = new SmallColumnStoreCache(SMALL_TABLE_THRESHOLD,
			SMALL_TABLES_CACHE_SIZE);

	// the size (in bytes) of the LRU cache for ColumnData of all tables
	private static final long COLUMN_DATA_CACHE_SIZE = 1L << 30; // 1 GB

	private static final CachedColumnReadStoreCache COLUMN_DATA_CACHE = new CachedColumnReadStoreCache(
			COLUMN_DATA_CACHE_SIZE);

	// how many batches of column data can be queued for for being asynchronously flushed to disk
	private static final int ASYNC_FLUSH_QUEUE_SIZE = 1000;

	private static final AsyncFlushColumnStoreExecutor ASYNC_FLUSH_EXECUTOR = new AsyncFlushColumnStoreExecutor(
			ASYNC_FLUSH_QUEUE_SIZE);

	private ColumnStoreUtils() {
	}

	public static ColumnStore cache(final ColumnStore store) {
		return new SmallColumnStore(
				new CachedColumnStore(new AsyncFlushColumnStore(store, ASYNC_FLUSH_EXECUTOR), COLUMN_DATA_CACHE),
				SMALL_TABLES_CACHE);
	}

	public static ColumnReadStore cache(final ColumnReadStore store) {
		return new CachedColumnReadStore(store, COLUMN_DATA_CACHE);
	}

}
