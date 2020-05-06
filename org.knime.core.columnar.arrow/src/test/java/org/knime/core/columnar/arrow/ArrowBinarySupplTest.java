package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;

import org.junit.Test;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.data.BinarySupplData;
import org.knime.core.columnar.data.IntData;

public class ArrowBinarySupplTest {

	@Test
	public void testBinarySupplement() throws Exception {
		int numChunks = 50;
		int chunkSize = 17;

		ArrowColumnStoreFactory factory = new ArrowColumnStoreFactory();

		final ColumnStoreSchema schema = new ColumnStoreSchema() {

			@Override
			public int getNumColumns() {
				return 1;
			}

			@Override
			public ColumnDataSpec<?> getColumnDataSpec(int idx) {
				return new BinarySupplData.BinarySupplDataSpec<>(new IntData.IntDataSpec());
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
			final BinarySupplData<IntData> cast = (BinarySupplData<IntData>) data[0];
			for (int i = 0; i < chunkSize; i++) {
				cast.getChunk().setInt(i, i);
				if (i % 2 == 0) {
					final byte[] test = new byte[5];
					for (int j = 0; j < test.length; j++) {
						test[j] = (byte) i;
					}
					cast.getBinarySupplData().setBytes(i, test);
				}
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
			final BinarySupplData<IntData> cast = (BinarySupplData<IntData>) data[0];
			for (int i = 0; i < chunkSize; i++) {
				assertEquals(i, cast.getChunk().getInt(i));
				if (i % 2 == 0) {
					byte[] test = cast.getBinarySupplData().getBytes(i);
					assertEquals(test.length, 5);
					for (int j = 0; j < test.length; j++) {
						assertEquals((byte) i, test[j]);
					}
				}
			}
			cast.release();
		}

		reader.close();
		store.close();
	}
}
