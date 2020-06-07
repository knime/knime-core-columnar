package org.knime.core.columnar.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelectionUtil;

class CacheTestUtils {

	static class TestColumnData implements ColumnData {

		private final int m_sizeOf;

		private int m_refs = 0;

		TestColumnData(final int sizeOf) {
			m_sizeOf = sizeOf;
		}

		@Override
		public void ensureCapacity(int capacity) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getMaxCapacity() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setNumValues(int numValues) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getNumValues() {
			throw new UnsupportedOperationException();
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

		int getRefs() {
			return m_refs;
		}

	}

	static ColumnStoreSchema createSchema(int numColumns) {
		return new ColumnStoreSchema() {
			@Override
			public int getNumColumns() {
				return numColumns;
			}

			@Override
			public ColumnDataSpec<?> getColumnDataSpec(int idx) {
				return null;
			}
		};
	}

	static int checkRefs(List<TestColumnData[]> table) {
		if (table.size() == 0) {
			return 0;
		}
		final int refs = checkRefs(table.get(0));
		for (final TestColumnData[] batch : table) {
			assertEquals(refs, checkRefs(batch));
		}
		return refs;
	}

	static int checkRefs(TestColumnData[] batch) {
		if (batch.length == 0) {
			return 0;
		}
		final int refs = batch[0].getRefs();
		for (final TestColumnData data : batch) {
			assertEquals(refs, data.getRefs());
		}
		return refs;
	}

	static void readAndCompareTable(ColumnStore store, List<TestColumnData[]> table) throws Exception {
		readSelectionAndCompareTable(store, table, null);
	}

	static void readSelectionAndCompareTable(ColumnStore store, List<TestColumnData[]> table, int... indices)
			throws Exception {
		try (final ColumnDataReader reader = store.createReader(ColumnSelectionUtil.create(indices))) {
			assertEquals(table.size(), reader.getNumChunks());
			for (int i = 0; i < reader.getNumChunks(); i++) {
				final TestColumnData[] written;
				if (indices == null) {
					written = table.get(i);
				} else {
					final TestColumnData[] all = table.get(i);
					written = Arrays.stream(indices).mapToObj(index -> all[index]).toArray(TestColumnData[]::new);
				}
				final ColumnData[] batch = reader.read(i);
				assertArrayEquals(written, batch);
				for (final ColumnData data : batch) {
					data.release();
				}
			}
		}
	}

	static void readTwiceAndCompareTable(ColumnStore store) throws Exception {
		try (final ColumnDataReader reader1 = store.createReader();
				final ColumnDataReader reader2 = store.createReader()) {
			assertEquals(reader1.getNumChunks(), reader2.getNumChunks());
			for (int i = 0; i < reader1.getNumChunks(); i++) {
				final ColumnData[] batch1 = reader1.read(i);
				final ColumnData[] batch2 = reader2.read(i);
				assertArrayEquals(batch1, batch2);
				for (final ColumnData data : batch1) {
					data.release();
					data.release();
				}
			}
		}
	}

	static boolean tableInStore(ColumnStore store, List<TestColumnData[]> table) throws Exception {
		try (final ColumnDataReader reader = store.createReader()) {
		} catch (IllegalStateException e) {
			return false;
		}
		return true;
	}

	static void writeTable(ColumnStore store, List<TestColumnData[]> table) throws Exception {
		try (final ColumnDataWriter writer = store.getWriter()) {
			for (final ColumnData[] batch : table) {
				writer.write(batch);
			}
		}
	}

	static List<TestColumnData[]> createTable(int height, int width, int size) {
		return IntStream.range(0, height).mapToObj(i -> createBatch(width, size)).collect(Collectors.toList());
	}

	static TestColumnData[] createBatch(int width, int size) {
		return IntStream.range(0, width).mapToObj(i -> new TestColumnData(size)).toArray(TestColumnData[]::new);
	}

}
