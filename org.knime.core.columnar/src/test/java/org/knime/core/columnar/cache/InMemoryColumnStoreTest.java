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

public class InMemoryColumnStoreTest {
	
	private static final int TABLE_HEIGHT = 2;
	private static final int TABLE_WIDTH = 2;
	private static final int SIZE_OF_COLUMN_DATA = 1;
	
	private static List<TestColumnData[]> generateTable() {
		return createTable(TABLE_HEIGHT, TABLE_WIDTH, SIZE_OF_COLUMN_DATA);
	}

	private static ColumnStoreSchema generateSchema() {
		return createSchema(TABLE_WIDTH);
	}
	
	@Test
	public void testSizeOf() throws Exception {
		
		final List<TestColumnData[]> table = generateTable();
		int sizeOf = 0;
		for (TestColumnData[] batch : table) {
			for (ColumnData data : batch) {
				sizeOf += data.sizeOf();
			}
		}

		try (final InMemoryColumnStore store = new InMemoryColumnStore(generateSchema())) {
			writeTable(store, table);
			assertEquals(sizeOf, store.sizeOf());
		}
	}
	
	@Test
	public void testRetain() throws Exception {
		
		final List<TestColumnData[]> table = generateTable();
		assertEquals(0, checkRefs(table));

		try (final InMemoryColumnStore store = new InMemoryColumnStore(generateSchema())) {
			writeTable(store, table);
			assertEquals(1, checkRefs(table));
			
			store.retain();
			assertEquals(2, checkRefs(table));
		}
	}
	
	@Test
	public void testRelease() throws Exception {
		
		final List<TestColumnData[]> table = generateTable();
		assertEquals(0, checkRefs(table));

		try (final InMemoryColumnStore store = new InMemoryColumnStore(generateSchema())) {
			writeTable(store, table);
			assertEquals(1, checkRefs(table));
			
			store.release();
			assertEquals(0, checkRefs(table));
		}
	}
	
	@Test
	public void testWriteRead() throws Exception {

		final List<TestColumnData[]> table = generateTable();
		assertEquals(0, checkRefs(table));

		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			writeTable(store, table);
			assertEquals(1, checkRefs(table));

			readAndCompareTable(store, table);
			assertEquals(1, checkRefs(table));
		}
		assertEquals(0, checkRefs(table));
	}
	
	@Test
	public void testWriteMultiRead() throws Exception {

		final List<TestColumnData[]> table = generateTable();
		assertEquals(0, checkRefs(table));

		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			writeTable(store, table);
			assertEquals(1, checkRefs(table));

			readTwiceAndCompareTable(store);
			assertEquals(1, checkRefs(table));
		}
		assertEquals(0, checkRefs(table));
	}
	
	@Test
	public void testWriteReadSelection() throws Exception {

		final List<TestColumnData[]> table = generateTable();
		assertEquals(0, checkRefs(table));

		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			writeTable(store, table);
			assertEquals(1, checkRefs(table));

			readSelectionAndCompareTable(store, table , 0);
			assertEquals(1, checkRefs(table));
		}
		assertEquals(0, checkRefs(table));
	}
	
	@Test
	public void testWriterSingleton() throws Exception {
		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			assertEquals(store.getWriter(), store.getWriter());
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnWriteAfterStoreClose() throws Exception {
		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			store.close();
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveWhileWriterOpen() throws Exception {
		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				store.saveToFile(null);
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnSaveAfterStoreClose() throws Exception {
		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				writer.write(createBatch(1, 1));
			}
			store.close();
			store.saveToFile(null);
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			try (final ColumnDataWriter writer = store.getWriter()) {
				try (final ColumnDataReader reader = store.createReader()) {
				}
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
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
		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
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
		try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
			store.close();
			store.getFactory();
		}
	}
}
