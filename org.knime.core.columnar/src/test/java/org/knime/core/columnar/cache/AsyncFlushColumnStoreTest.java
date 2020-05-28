package org.knime.core.columnar.cache;

import static org.junit.Assert.assertEquals;
import static org.knime.core.columnar.cache.CacheTestUtils.checkRefs;
import static org.knime.core.columnar.cache.CacheTestUtils.createBatch;
import static org.knime.core.columnar.cache.CacheTestUtils.createSchema;
import static org.knime.core.columnar.cache.CacheTestUtils.createTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.writeTable;

import java.util.List;

import org.junit.Test;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.CacheTestUtils.TestColumnData;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

public class AsyncFlushColumnStoreTest {

	private static final int tableHeight = 2;
	private static final int tableWidth = 2;
	private static final int sizeOfColumnData = 1;

	private List<TestColumnData[]> generateTable() {
		return createTable(tableHeight, tableWidth, sizeOfColumnData);
	}

	private ColumnStoreSchema generateSchema() {
		return createSchema(tableWidth);
	}

	@Test
	public void testWriteRead() throws Exception {

		final List<TestColumnData[]> table = generateTable();
		assertEquals(0, checkRefs(table));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate)) {
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
				final AsyncFlushColumnStore store = new AsyncFlushColumnStore(delegate)) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				for (final ColumnData[] batch : table) {
					writer.write(batch);
					store.waitForFutureAndLogExceptions();
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
				final ColumnStore store = new AsyncFlushColumnStore(delegate)) {
			assertEquals(store.getWriter(), store.getWriter());
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnWriteAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate)) {
			store.close();
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveWhileWriterOpen() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate)) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				store.saveToFile(null);
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate)) {
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
				final ColumnStore store = new AsyncFlushColumnStore(delegate)) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				try (final ColumnDataReader reader = store.createReader(() -> null)) {
				}
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate)) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
			store.close();
			try (final ColumnDataReader reader = store.createReader(() -> null)) {
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new AsyncFlushColumnStore(delegate)) {
			store.close();
			store.getFactory();
		}
	}
}
