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
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.CacheTestUtils.TestColumnData;
import org.knime.core.columnar.cache.CachedColumnReadStore.CachedColumnReadStoreCache;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

public class CachedColumnStoreTest {
	
	private static final int tableHeight = 2;
	private static final int tableWidth = 2;
	private static final int sizeOfColumnData = 1;
	private static final int sizeOfTable = tableHeight * tableWidth * sizeOfColumnData;

	private CachedColumnReadStoreCache generateCache() {
		return new CachedColumnReadStoreCache(sizeOfTable);
	}

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
				final ColumnStore store = new CachedColumnStore(delegate, generateCache())) {

			writeTable(store, table);
			assertEquals(2, checkRefs(table)); // held once in the delegate and once in the store

			readAndCompareTable(store, table);
			assertEquals(2, checkRefs(table));
		}
		assertEquals(0, checkRefs(table));
	}
	
	@Test
	public void testWriteMultiRead() throws Exception {

		final List<TestColumnData[]> smallTable = generateTable();
		assertEquals(0, checkRefs(smallTable));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new CachedColumnStore(delegate, generateCache())) {

			writeTable(store, smallTable);
			assertEquals(2, checkRefs(smallTable));

			readTwiceAndCompareTable(store);
			assertEquals(2, checkRefs(smallTable));
		}
		assertEquals(0, checkRefs(smallTable));
	}
	
	@Test
	public void testWriteReadSelection() throws Exception {

		final List<TestColumnData[]> smallTable = generateTable();
		assertEquals(0, checkRefs(smallTable));

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new CachedColumnStore(delegate, generateCache())) {

			writeTable(store, smallTable);
			assertEquals(2, checkRefs(smallTable));

			readSelectionAndCompareTable(store, smallTable, 0);
			assertEquals(2, checkRefs(smallTable));
		}
		assertEquals(0, checkRefs(smallTable));
	}
	
	@Test
	public void testWriteEvictRead() throws Exception {

		final List<TestColumnData[]> table1 = generateTable();
		final List<TestColumnData[]> table2 = generateTable();
		assertEquals(0, checkRefs(table1));
		assertEquals(0, checkRefs(table2));

		final ColumnStoreSchema schema = generateSchema();
		final CachedColumnReadStoreCache cache = generateCache();

		try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
				final ColumnStore store1 = new CachedColumnStore(delegate1, cache);
				final ColumnStore delegate2 = new InMemoryColumnStore(schema);
				final ColumnStore store2 = new CachedColumnStore(delegate2, cache)) {

			writeTable(store1, table1);
			assertEquals(2, checkRefs(table1));
			writeTable(store2, table2);
			assertEquals(1, checkRefs(table1));
			assertEquals(2, checkRefs(table2));

			readAndCompareTable(store1, table1);
			assertEquals(2, checkRefs(table1));
			readAndCompareTable(store2, table2);
			assertEquals(2, checkRefs(table2));
		}
		assertEquals(0, checkRefs(table1));
		assertEquals(0, checkRefs(table2));
	}
	
	
	@Test
	public void testCacheEmptyAfterClear() throws Exception {
		
		final List<TestColumnData[]> table = generateTable();
		final CachedColumnReadStoreCache cache = generateCache();

		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new CachedColumnStore(delegate, cache)) {

			writeTable(store, table);
			assertEquals(sizeOfTable, cache.size());
		}
		assertEquals(0, cache.size());
	}
	
	@Test
	public void testWriterSingleton() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new CachedColumnStore(delegate, generateCache())) {
			assertEquals(store.getWriter(), store.getWriter());
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnWriteAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
			final ColumnStore store = new CachedColumnStore(delegate, generateCache());
			store.close();
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveWhileWriterOpen() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
				final ColumnStore store = new CachedColumnStore(delegate, generateCache())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				store.saveToFile(null);
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
			final ColumnStore store = new CachedColumnStore(delegate, generateCache());
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
				final ColumnStore store = new CachedColumnStore(delegate, generateCache())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				try (final ColumnDataReader reader = store.createReader()) {
				}
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
		try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
			final ColumnStore store = new CachedColumnStore(delegate, generateCache());
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
			final ColumnStore store = new CachedColumnStore(delegate, generateCache());
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
			final ColumnStore store = new CachedColumnStore(delegate, generateCache());
			store.close();
			store.getFactory();
		}
	}

}
