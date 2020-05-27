package org.knime.core.columnar.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

public class InMemoryColumnStoreTest {

	@Test
	public void test() throws Exception {

		final TestColumnData[] written = new TestColumnData[] { new TestColumnData(2), new TestColumnData(2) };

		try (ColumnStore store = new InMemoryColumnStore(TestColumnData.createSchema(written.length))) {

			try (ColumnDataWriter writer = store.getWriter()) {
				writer.write(written);
			}

			try (ColumnDataReader reader = store.createReader(() -> null)) {
				for (int i = 0; i < reader.getNumEntries(); i++) {
					ColumnData[] read = reader.read(i);
					assertArrayEquals(written, read);
					for (ColumnData data : read) {
						data.release(); // all TableStores in this package only hand out retained data
					}
				}
			}
		}

		for (TestColumnData data : written) {
			assertEquals(0, data.getRefs());
		}
	}

}
