package org.knime.core.columnar.cache;

import static org.junit.Assert.assertEquals;
import static org.knime.core.columnar.cache.CacheTestUtils.checkRefs;
import static org.knime.core.columnar.cache.CacheTestUtils.createBatch;
import static org.knime.core.columnar.cache.CacheTestUtils.createSchema;
import static org.knime.core.columnar.cache.CacheTestUtils.createTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.writeTable;

import java.util.List;

import org.junit.Test;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.AsyncFlushColumnStore.AsyncFlushColumnStoreExecutor;
import org.knime.core.columnar.cache.CacheTestUtils.TestColumnData;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

public class AsyncFlushColumnStoreTest {

	private static final int TABLE_HEIGHT = 4;
	private static final int TABLE_WIDTH = 2;
	private static final int SIZE_OF_COLUMN_DATA = 1;
	private static final int ASYNC_ECECUTOR_QUEUE_SIZE = 2;

	private static List<TestColumnData[]> generateTable() {
		return createTable(TABLE_HEIGHT, TABLE_WIDTH, SIZE_OF_COLUMN_DATA);
	}

	private static ColumnStoreSchema generateSchema() {
		return createSchema(TABLE_WIDTH);
	}

	private static AsyncFlushColumnStoreExecutor generateExecutor() {
		return new AsyncFlushColumnStoreExecutor(ASYNC_ECECUTOR_QUEUE_SIZE);
	}

	@Test
	public void testWriteRead() throws Exception {

		final List<TestColumnData[]> table = generateTable();
		assertEquals(0, checkRefs(table));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			writeTable(store, table);
			assertEquals(1, checkRefs(table));

			readAndCompareTable(store, table);
			assertEquals(1, checkRefs(table));
		}
		assertEquals(0, checkRefs(table));
	}

	@Test
	public void testWriteWaitWriteRead() throws Exception {

		final List<TestColumnData[]> table = generateTable();
		assertEquals(0, checkRefs(table));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final AsyncFlushColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				for (final ColumnData[] batch : table) {
					writer.write(batch);
					store.waitForAndHandleFutures();
				}
			}
			assertEquals(1, checkRefs(table));

			readAndCompareTable(store, table);
			assertEquals(1, checkRefs(table));
		}
		assertEquals(0, checkRefs(table));
	}

	@Test
	public void testWriterSingleton() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			assertEquals(store.getWriter(), store.getWriter());
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnWriteAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			store.close();
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveWhileWriterOpen() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				store.saveToFile(null);
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
			store.close();
			store.saveToFile(null);
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				try (final ColumnDataReader reader = store.createReader()) {
				}
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
			store.close();
			try (final ColumnDataReader reader = store.createReader()) {
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate, generateExecutor())) {
			store.close();
			store.getFactory();
		}
	}
}
