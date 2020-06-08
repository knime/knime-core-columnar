package org.knime.core.columnar.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.cache.CacheTestUtils.checkRefs;
import static org.knime.core.columnar.cache.CacheTestUtils.createBatch;
import static org.knime.core.columnar.cache.CacheTestUtils.createSchema;
import static org.knime.core.columnar.cache.CacheTestUtils.createTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.tableInStore;
import static org.knime.core.columnar.cache.CacheTestUtils.writeTable;

import java.util.List;

import org.junit.Test;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.CacheTestUtils.TestColumnData;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

public class SmallColumnStoreTest {
	
	private static final int SMALL_TABLE_HEIGHT = 2;
	private static final int SMALL_TABLE_WIDTH = 2;
	private static final int SIZE_OF_COLUMN_DATA = 1;

	private static SmallColumnStoreCache generateCache() {
		final int sizeOfSmallTable = SMALL_TABLE_HEIGHT * SMALL_TABLE_HEIGHT * SIZE_OF_COLUMN_DATA;
		return new SmallColumnStoreCache(sizeOfSmallTable, sizeOfSmallTable);
	}

	private static List<TestColumnData[]> generateSmallTable() {
		return createTable(SMALL_TABLE_HEIGHT, SMALL_TABLE_WIDTH, SIZE_OF_COLUMN_DATA);
	}

	private static List<TestColumnData[]> generate2SmallTableSizedTable() {
		return createTable(SMALL_TABLE_HEIGHT * 2, SMALL_TABLE_WIDTH, SIZE_OF_COLUMN_DATA);
	}

	private static ColumnStoreSchema generateSchema() {
		return createSchema(SMALL_TABLE_WIDTH);
	}

	@Test
	public void testSmallWriteRead() throws Exception {

		final List<TestColumnData[]> smallTable = generateSmallTable();
		assertEquals(0, checkRefs(smallTable));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {

			writeTable(store, smallTable);
			assertFalse(tableInStore(delegate, smallTable));
			assertEquals(1, checkRefs(smallTable));

			readAndCompareTable(store, smallTable);
			assertEquals(1, checkRefs(smallTable));
		}
		assertEquals(0, checkRefs(smallTable));
	}

	@Test
	public void testSmallWriteMultiRead() throws Exception {

		final List<TestColumnData[]> smallTable = generateSmallTable();
		assertEquals(0, checkRefs(smallTable));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {

			writeTable(store, smallTable);
			assertFalse(tableInStore(delegate, smallTable));
			assertEquals(1, checkRefs(smallTable));

			readTwiceAndCompareTable(store);
			assertEquals(1, checkRefs(smallTable));
		}
		assertEquals(0, checkRefs(smallTable));
	}
	
	@Test
	public void testSmallWriteReadSelection() throws Exception {

		final List<TestColumnData[]> smallTable = generateSmallTable();
		assertEquals(0, checkRefs(smallTable));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {

			writeTable(store, smallTable);
			assertFalse(tableInStore(delegate, smallTable));
			assertEquals(1, checkRefs(smallTable));

			readSelectionAndCompareTable(store, smallTable, 0);
			assertEquals(1, checkRefs(smallTable));
		}
		assertEquals(0, checkRefs(smallTable));
	}

	@Test
	public void testLargeWriteRead() throws Exception {

		final List<TestColumnData[]> largeTable = generate2SmallTableSizedTable();
		assertEquals(0, checkRefs(largeTable));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {

			writeTable(store, largeTable);
			assertTrue(tableInStore(delegate, largeTable));
			assertEquals(1, checkRefs(largeTable));

			readAndCompareTable(store, largeTable);
			assertEquals(1, checkRefs(largeTable));
		}
		assertEquals(0, checkRefs(largeTable));
	}

	@Test
	public void testSmallWriteEvictRead() throws Exception {

		final List<TestColumnData[]> smallTable1 = generateSmallTable();
		final List<TestColumnData[]> smallTable2 = generateSmallTable();
		assertEquals(0, checkRefs(smallTable1));
		assertEquals(0, checkRefs(smallTable2));

		final ColumnStoreSchema schema = generateSchema();
		final SmallColumnStoreCache cache = generateCache();

		try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
				final ColumnStore store1 = new SmallColumnStore(delegate1, cache);
				final ColumnStore delegate2 = new InMemoryColumnStore(schema);
				final ColumnStore store2 = new SmallColumnStore(delegate2, cache)) {

			writeTable(store1, smallTable1);
			assertFalse(tableInStore(delegate1, smallTable1));
			assertEquals(1, checkRefs(smallTable1));
			writeTable(store2, smallTable2);
			assertTrue(tableInStore(delegate1, smallTable1));
			assertEquals(1, checkRefs(smallTable1));
			assertFalse(tableInStore(delegate2, smallTable2));
			assertEquals(1, checkRefs(smallTable2));

			readAndCompareTable(store1, smallTable1);
			assertEquals(1, checkRefs(smallTable1));
			readAndCompareTable(store2, smallTable2);
			assertEquals(1, checkRefs(smallTable2));
		}
		assertEquals(0, checkRefs(smallTable1));
		assertEquals(0, checkRefs(smallTable2));
	}

	@Test
	public void testSmallWriteEvictSaveRead() throws Exception {

		final List<TestColumnData[]> smallTable1 = generateSmallTable();
		final List<TestColumnData[]> smallTable2 = generateSmallTable();
		assertEquals(0, checkRefs(smallTable1));
		assertEquals(0, checkRefs(smallTable2));

		final ColumnStoreSchema schema = generateSchema();
		final SmallColumnStoreCache cache = generateCache();

		try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
				final ColumnStore store1 = new SmallColumnStore(delegate1, cache);
				final ColumnStore delegate2 = new InMemoryColumnStore(schema);
				final ColumnStore store2 = new SmallColumnStore(delegate2, cache)) {

			writeTable(store1, smallTable1);
			assertFalse(tableInStore(delegate1, smallTable1));
			assertEquals(1, checkRefs(smallTable1));
			writeTable(store2, smallTable2);
			assertTrue(tableInStore(delegate1, smallTable1));
			assertEquals(1, checkRefs(smallTable1));
			assertFalse(tableInStore(delegate2, smallTable2));
			assertEquals(1, checkRefs(smallTable2));

			try {
				store1.saveToFile(null);
			} catch (UnsupportedOperationException e) {
			}
			try {
				store2.saveToFile(null);
			} catch (UnsupportedOperationException e) {
			}

			readAndCompareTable(store1, smallTable1);
			assertEquals(1, checkRefs(smallTable1));
			readAndCompareTable(store2, smallTable2);
			assertEquals(2, checkRefs(smallTable2));
		}
		assertEquals(0, checkRefs(smallTable1));
		assertEquals(0, checkRefs(smallTable2));
	}

	@Test
	public void testSmallWriteSaveEvictRead() throws Exception {

		final List<TestColumnData[]> smallTable1 = generateSmallTable();
		final List<TestColumnData[]> smallTable2 = generateSmallTable();
		assertEquals(0, checkRefs(smallTable1));
		assertEquals(0, checkRefs(smallTable2));

		final ColumnStoreSchema schema = generateSchema();
		final SmallColumnStoreCache cache = generateCache();

		try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
				final ColumnStore store1 = new SmallColumnStore(delegate1, cache);
				final ColumnStore delegate2 = new InMemoryColumnStore(schema);
				final ColumnStore store2 = new SmallColumnStore(delegate2, cache)) {

			writeTable(store1, smallTable1);
			assertFalse(tableInStore(delegate1, smallTable1));
			assertEquals(1, checkRefs(smallTable1));

			try {
				store1.saveToFile(null);
			} catch (UnsupportedOperationException e) {
			}

			writeTable(store2, smallTable2);
			assertTrue(tableInStore(delegate1, smallTable1));
			assertEquals(1, checkRefs(smallTable1));
			assertFalse(tableInStore(delegate2, smallTable2));
			assertEquals(1, checkRefs(smallTable2));

			try {
				store2.saveToFile(null);
			} catch (UnsupportedOperationException e) {
			}

			readAndCompareTable(store1, smallTable1);
			assertEquals(1, checkRefs(smallTable1));
			readAndCompareTable(store2, smallTable2);
			assertEquals(2, checkRefs(smallTable2));
		}
		assertEquals(0, checkRefs(smallTable1));
		assertEquals(0, checkRefs(smallTable2));
	}

	@Test
	public void testCacheEmptyAfterClear() throws Exception {
		
		final List<TestColumnData[]> smallTable = generateSmallTable();
		final SmallColumnStoreCache cache = generateCache();

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new SmallColumnStore(delegate, cache)) {

			writeTable(store, smallTable);
			assertEquals(1, cache.size());
		}
		assertEquals(0, cache.size());
	}
	
	@Test
	public void testWriterSingleton() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {
			assertEquals(store.getWriter(), store.getWriter());
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnWriteAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
			final ColumnStore store = new SmallColumnStore(delegate, generateCache());
			store.close();
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveWhileWriterOpen() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				store.saveToFile(null);
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
			final ColumnStore store = new SmallColumnStore(delegate, generateCache());
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
				final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				try (final ColumnDataReader reader = store.createReader()) {
				}
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
			final ColumnStore store = new SmallColumnStore(delegate, generateCache());
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
			store.close();
			try (final ColumnDataReader reader = store.createReader()) {
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnReadAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
			final ColumnStore store = new SmallColumnStore(delegate, generateCache());
			try (ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
			try (final ColumnDataReader reader = store.createReader()) {
				store.close();
				reader.read(0);
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
			final ColumnStore store = new SmallColumnStore(delegate, generateCache());
			store.close();
			store.getFactory();
		}
	}
}
