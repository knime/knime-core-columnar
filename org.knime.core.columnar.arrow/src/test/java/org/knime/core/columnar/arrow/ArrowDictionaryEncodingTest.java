package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;

import org.junit.Test;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.data.BinarySupplData;
import org.knime.core.columnar.data.StringData;

public class ArrowDictionaryEncodingTest extends AbstractArrowTest {

	@Test
	public void testDictionaryEncoding() throws Exception {
		int numChunks = 10;
		int chunkSize = 1_000;

		final ArrowColumnStoreFactory factory = new ArrowColumnStoreFactory();
		final ColumnStoreSchema schema = new ColumnStoreSchema() {

			@Override
			public int getNumColumns() {
				return 1;
			}

			@Override
			public ColumnDataSpec<?> getColumnDataSpec(int idx) {
				return new StringData.StringDataSpec(true);
			}
		};

		final ColumnStore store = factory.createWriteStore(schema, Files.createTempFile("test", ".knarrow").toFile(),
				chunkSize);

		// let's store some data
		final ColumnDataWriter writer = store.getWriter();
		ColumnDataFactory fac = store.getFactory();
		for (int c = 0; c < numChunks; c++) {
			final ColumnData[] data = fac.create();
			final StringData cast = (StringData) data[0];
			for (int i = 0; i < chunkSize; i++) {
				cast.setString(i, "Test" + i * c);
			}
			cast.setNumValues(chunkSize);
			writer.write(data);
			cast.release();
		}
		writer.close();

		// let's read some data back
		final ColumnDataReader reader = store.createReader();
		for (int c = 0; c < numChunks; c++) {
			final ColumnData[] data = reader.read(c);
			final StringData cast = (StringData) data[0];
			for (int i = 0; i < chunkSize; i++) {
				assertEquals("Test" + i * c, cast.getString(i));
			}
			cast.release();
		}

		reader.close();
		store.close();
	}

	@Test
	public void testBinarySupplementWithDictEncoding() throws Exception {
		int numChunks = 32;
		int chunkSize = 17;

		ArrowColumnStoreFactory factory = new ArrowColumnStoreFactory();

		final ColumnStoreSchema schema = new ColumnStoreSchema() {

			@Override
			public int getNumColumns() {
				return 1;
			}

			@Override
			public ColumnDataSpec<?> getColumnDataSpec(int idx) {
				return new BinarySupplData.BinarySupplDataSpec<>(new StringData.StringDataSpec(true));
			}
		};

		final ArrowColumnStore store = factory.createWriteStore(schema,
				Files.createTempFile("test", ".knarrow").toFile(), chunkSize);

		// let's store some data
		final ColumnDataWriter writer = store.getWriter();
		ColumnDataFactory dataFac = store.getFactory();
		for (int c = 0; c < numChunks; c++) {
			final ColumnData[] data = dataFac.create();
			@SuppressWarnings("unchecked")
			final BinarySupplData<StringData> cast = (BinarySupplData<StringData>) data[0];
			for (int i = 0; i < chunkSize; i++) {
				cast.getChunk().setString(i, "Test " + i);
			}
			cast.setNumValues(chunkSize);
			writer.write(data);
			cast.release();
		}
		writer.close();

		// let's read some data back
		final ColumnDataReader reader = store.createReader();
		for (int c = 0; c < numChunks; c++) {
			final ColumnData[] data = reader.read(c);
			@SuppressWarnings("unchecked")
			final BinarySupplData<StringData> cast = (BinarySupplData<StringData>) data[0];
			for (int i = 0; i < chunkSize; i++) {
				assertEquals("Test " + i, cast.getChunk().getString(i));
			}
			cast.release();
		}
		reader.close();
		store.close();
	}
}
