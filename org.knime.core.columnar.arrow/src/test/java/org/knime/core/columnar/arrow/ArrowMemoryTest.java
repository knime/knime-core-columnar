package org.knime.core.columnar.arrow;

public class ArrowMemoryTest extends AbstractArrowTest {

	/*
	 * Disabled on purpose. long-running test.
	 */

	// @Test
//	public void testCopy() throws Exception {
//
//		final ExecutorService pool = Executors.newFixedThreadPool(32);
//
//		for (int l = 0; l < 128; l++) {
//			System.out.println("Running loop " + l);
//
//			pool.submit(new Callable<Void>() {
//
//				@Override
//				public Void call() throws Exception {
//					int numChunks = 196;
//					int chunkSize = 16_000;
//
//					final ColumnStoreFactory factory = new ArrowColumnStoreFactory();
//
//					final ColumnStoreSchema schema = new ColumnStoreSchema() {
//
//						@Override
//						public int getNumColumns() {
//							return 33;
//						}
//
//						@Override
//						public ColumnDataSpec<?> getColumnDataSpec(int idx) {
//							return new DoubleData.DoubleDataSpec();
//						}
//					};
//
//					final ColumnStore store = ColumnStoreUtils.cache(factory.createWriteStore(schema,
//							Files.createTempFile("test", ".knarrow").toFile(), chunkSize));
//
//					// let's store some data
//					final ColumnDataWriter writer = store.getWriter();
//					ColumnDataFactory fac = store.getFactory();
//					for (int c = 0; c < numChunks; c++) {
//						ColumnData[] chunks = fac.create();
//						for (ColumnData data : chunks) {
//							for (int i = 0; i < chunkSize; i++) {
//								((DoubleData) data).setDouble(i, i);
//							}
//							data.setNumValues(chunkSize);
//						}
//						writer.write(chunks);
//						for (ColumnData data : chunks) {
//							data.release();
//						}
//					}
//					writer.close();
//
//					final ColumnStore copyStore = ColumnStoreUtils.cache(factory.createWriteStore(schema,
//							Files.createTempFile("test", ".knarrow").toFile(), chunkSize));
//
//					// write
//
//					// let's read some data back
//					final ColumnDataReader reader = store.createReader();
//					final ColumnDataWriter copyWriter = copyStore.getWriter();
//					for (int c = 0; c < numChunks; c++) {
//						final ColumnData[] chunks = reader.read(c);
//						copyWriter.write(chunks);
//						for (ColumnData data : chunks) {
//							data.release();
//						}
//					}
//					writer.close();
//					copyStore.close();
//					reader.close();
//					store.close();
//					return null;
//				}
//
//			}).get();
//		}
//	}
}
