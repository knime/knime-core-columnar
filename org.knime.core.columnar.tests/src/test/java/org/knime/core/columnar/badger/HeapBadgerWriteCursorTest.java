/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   20 Dec 2023 (chaubold): created
 */
package org.knime.core.columnar.badger;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.access.StructAccess.StructWriteAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryWriteAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @author Tobias Pietzsch
 */
class HeapBadgerWriteCursorTest {

    // Tests (always check that the heap cache is filled)
    // - One batch that does not have max size (but is written)
    // - One batch that just enough - would switch to new batch with next data but should not
    // - Two batches - just switched to next data by one row because of max rows
    // - Two batches - just switched to next data by one row because of max size
    // - 4 batches
    // - Data so big that one row already surpasses the max size
    // - Settings such that one row of fixed data would surpass the max size
    // - Test exception during serialization
    // TODO we should also test edge cases of the buffer configuration

    @Test
    @DisplayName("no batching - int|string")
    void testSingleBatchStringInt() throws IOException {
        runFillAndCheckHeapBadgerTest( //
            25, // num rows
            26, // max num rows per batch
            Integer.MAX_VALUE, // max batch size in bytes
            new int[]{25}, // expected num rows
            new int[][]{new int[]{}, //no ints cached
                new int[]{0} //one string batch cached
            }, // expected cache indices
            TestDataImpl.INT, TestDataImpl.STRING // test data
        );
    }

    @Test
    @DisplayName("batching by num rows - int|string")
    void testTwoBatchesStringInt() throws IOException {
        runFillAndCheckHeapBadgerTest( //
            25, // num rows
            24, // max num rows per batch
            Integer.MAX_VALUE, // max batch size in bytes
            new int[]{24, 1}, // expected num rows
            new int[][]{new int[]{}, //no ints cached
                new int[]{0} //one string batch cached
            }, // expected cache indices
            TestDataImpl.INT, TestDataImpl.STRING // test data
        );
    }

    @Test
    @DisplayName("batching by size - int|string")
    void testTwoBatchesBySizeStringInt() throws IOException {
        // NB: The size of the test data is just the length
        runFillAndCheckHeapBadgerTest( //
            26, // num rows
            1000, // max num rows per batch
            48, // max batch size in bytes
            new int[]{24, 2}, // expected num rows
            new int[][]{new int[]{}, //no ints cached
                new int[]{0} //one string batch cached
            }, // expected cache indices
            TestDataImpl.INT, TestDataImpl.STRING // test data
        );
    }

    @Test
    @DisplayName("batching by initial size of many columns")
    void testInitialSizeLimitsMaxRows() throws IOException {
        runFillAndCheckHeapBadgerTest( //
            100, // num rows
            1_000_000, // max num rows per batch
            1024, // max batch size in bytes
            new int[]{51, 49}, // expected num rows
            new int[][]{}, IntStream.range(0, 20).mapToObj(i -> TestDataImpl.STRING).toArray(TestData[]::new) // test data
        );
    }

    @Test
    @DisplayName("write - flush - write")
    void testWriteAfterFlush() throws IOException {
        runFillAndCheckHeapBadgerTest( //
            25, // num rows
            24, // max num rows per batch
            Integer.MAX_VALUE, // max batch size in bytes
            new int[]{24, 1}, // expected num rows
            new int[][]{new int[]{}, //no ints cached
                new int[]{0} //one string batch cached
            }, // expected cache indices
            (row, cursor) -> {
                if (row == 10) {
                    try {
                        cursor.flush();
                    } catch (IOException ex) {
                        fail("Flushing should not fail");
                    }
                }
                return true;
            }, TestDataImpl.INT, TestDataImpl.STRING // test data
        );
    }

    @Test
    @DisplayName("close while serialization is waiting")
    @Timeout(60)
    void testCloseWithoutFlushDuringSerializationWait() throws IOException {
        runFillAndCheckHeapBadgerTest( //
            25, // num rows
            24, // max num rows per batch
            Integer.MAX_VALUE, // max batch size in bytes
            new int[]{24, 1}, // expected num rows
            new int[][]{new int[]{}, //no ints cached
                new int[]{0} //one string batch cached
            }, // expected cache indices
            (row, cursor) -> {
                if (row == 10) {
                    try {
                        try {
                            // This causes the AsyncQueue to finish all pending serialization and wait in
                            // `m_notEmpty.await();` - we should still be able to close the cursor
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            Assertions.fail("Waiting for the serialization loop to catch up was interrupted.", ex);
                        }
                        cursor.close();
                        return false;
                    } catch (IOException ex) {
                        fail("Closing should not fail");
                    }
                }
                return true;
            }, TestDataImpl.INT, TestDataImpl.STRING // test data
        );
    }

    @Test
    @DisplayName("close without flush or finish")
    void testCloseWithoutFlush() throws IOException {
        // TODO: try to make me fail :D
        runFillAndCheckHeapBadgerTest( //
            25, // num rows
            24, // max num rows per batch
            Integer.MAX_VALUE, // max batch size in bytes
            new int[]{24, 1}, // expected num rows
            new int[][]{new int[]{}, //no ints cached
                new int[]{0} //one string batch cached
            }, // expected cache indices
            (row, cursor) -> {
                if (row == 10) {
                    try {
                        cursor.close();
                        return false;
                    } catch (IOException ex) {
                        fail("Closing should not fail");
                    }
                }
                return true;
            }, TestDataImpl.INT, TestDataImpl.STRING // test data
        );
    }

    @Test
    @DisplayName("error handling - failing serializer")
    @Timeout(1)
    void testFailingSerializer() throws IOException {
        try {
            runFillAndCheckHeapBadgerTest(25, 100, Integer.MAX_VALUE, new int[]{25}, new FailingSerializeObjectData());
        } catch (IOException e) {
            assertEquals("Error during serialization: This serializer is buggy", e.getMessage());
            return;
        }
        fail("Expected an exception to be thrown");
    }

    @Test
    @DisplayName("error handling - interrupted serializer")
    @Timeout(1)
    void testInterruptedSerializer() throws IOException {
        try {
            Thread.currentThread().interrupt(); // to cause InterruptExceptions on await
            runFillAndCheckHeapBadgerTest(25, 100, Integer.MAX_VALUE, new int[]{25}, TestDataImpl.INT);
        } catch (RuntimeException e) {
            assertEquals("java.lang.InterruptedException", e.getMessage());
            return;
        }
        fail("Expected an exception to be thrown");
    }

    @ParameterizedTest
    @EnumSource(value = TestDataImpl.class)
    void testOneBatchWithTypes(final TestData data) throws IOException {
        runFillAndCheckHeapBadgerTest(10, 20, Integer.MAX_VALUE, new int[]{10}, data);
    }

    // ====== TEST UTILITIES

    interface RowCallback {
        boolean keepGoing(Long rowIdx, WriteCursor<WriteAccessRow> cursor);
    }

    static class MockSharedObjectCache implements SharedObjectCache {
        private Map<ColumnDataUniqueId, Object> m_cache = new HashMap<>();

        @Override
        public Object computeIfAbsent(final ColumnDataUniqueId key,
            final Function<ColumnDataUniqueId, Object> mappingFunction) {
            return m_cache.computeIfAbsent(key, mappingFunction);
        }

        @Override
        public void put(final ColumnDataUniqueId key, final Object value) {
            m_cache.put(key, value);
        }

        @Override
        public void removeAll(final Collection<ColumnDataUniqueId> keys) {
            keys.stream().forEach(m_cache::remove);
        }

        Map<ColumnDataUniqueId, Object> getContents() {
            return m_cache;
        }
    }

    private static void runFillAndCheckHeapBadgerTest( //
        final int numRows, //
        final int maxNumRowsPerBatch, //
        final int maxBatchSizeInBytes, //
        final int[] expectedNumRows, //
        final TestData... testData //
    ) throws IOException {
        runFillAndCheckHeapBadgerTest(numRows, //
            maxNumRowsPerBatch, //
            maxBatchSizeInBytes, //
            expectedNumRows, //
            new int[0][], //
            testData);
    }

    private static void runFillAndCheckHeapBadgerTest( //
        final int numRows, //
        final int maxNumRowsPerBatch, //
        final int maxBatchSizeInBytes, //
        final int[] expectedNumRows, //
        final int[][] expectedCacheDataIndices, //
        final TestData... testData //
    ) throws IOException {
        runFillAndCheckHeapBadgerTest(numRows, maxNumRowsPerBatch, maxBatchSizeInBytes, expectedNumRows,
            expectedCacheDataIndices, (a, b) -> {
                return true;
            }, testData);
    }

    @SuppressWarnings("resource") // heap cache doesn't need to be closed by us
    private static void runFillAndCheckHeapBadgerTest( //
        final int numRows, //
        final int maxNumRowsPerBatch, //
        final int maxBatchSizeInBytes, //
        final int[] expectedNumRows, //
        final int[][] expectedCacheDataIndices, //
        final RowCallback rowCallback, // is called after writing each row
        final TestData... testData //
    ) throws IOException {
        try (TestBatchStore batchStore = createTestStore(testData)) {
            var cache = new MockSharedObjectCache();
            var badger = new HeapBadger(batchStore, batchStore, maxNumRowsPerBatch, maxBatchSizeInBytes, cache,
                null);
            if (!writeToHeapBadger(badger, testData, numRows, rowCallback)) {
                return;
            }

            assertWrittenData(testData, expectedNumRows, batchStore);
            if (expectedCacheDataIndices.length > 0) {
                assertDataInCache(cache, badger.getHeapCache(), expectedCacheDataIndices);
                assertWrittenData(testData, expectedNumRows, badger.getHeapCache(), batchStore.numBatches());
            }
        }
    }

    /**
     * @param cache
     * @param expectedCacheDataIndices
     */
    private static void assertDataInCache(final MockSharedObjectCache cache, final HeapCache heapCache,
        final int[][] expectedCacheDataIndices) {
        for (int colIdx = 0; colIdx < expectedCacheDataIndices.length; colIdx++) {
            if (expectedCacheDataIndices[colIdx] == null) {
                continue;
            }

            for (int batchIdx : expectedCacheDataIndices[colIdx]) {
                if (!cache.getContents().containsKey(new ColumnDataUniqueId(heapCache, colIdx, batchIdx))) {
                    throw new AssertionError("Could not find column=" + colIdx + " batch=" + batchIdx + " in cache");
                }
            }
        }
    }

    /** Create a new batch store that can store the given test data */
    private static TestBatchStore createTestStore(final TestData[] data) {
        var schema = ColumnarSchema.of(Arrays.stream(data).map(d -> d.getSpec()).toArray(DataSpecWithTraits[]::new));
        return TestBatchStore.create(schema);
    }

    /** Write some test data to the HeapBadger */
    private static boolean writeToHeapBadger(final HeapBadger badger, final TestData[] data, final long numRows,
        final RowCallback rowCallback) throws IOException {
        try (WriteCursor<WriteAccessRow> cursor = badger.getWriteCursor()) {
            for (long rowIdx = 0; rowIdx < numRows; rowIdx++) {
                for (int col = 0; col < data.length; col++) {
                    data[col].writeTo(cursor.access().getWriteAccess(col), rowIdx);
                }
                if (!rowCallback.keepGoing(rowIdx, cursor)) { // TODO (TP): should this be done after commit()?
                    return false;
                }
                cursor.commit();
            }
            cursor.finish();
        }
        return true;
    }

    /** Check the written data in the underlying store */
    private static void assertWrittenData(final TestData[] testData, final int[] numRowsPerBatch,
        final TestBatchStore batchStore) throws IOException {
        assertWrittenData(testData, numRowsPerBatch, batchStore, batchStore.numBatches());
    }

    /** Check the written data in the numBatche of the batch readable */
    private static void assertWrittenData(final TestData[] testData, final int[] numRowsPerBatch,
        final RandomAccessBatchReadable batchReadable, final int numBatches) throws IOException {
        var numColumns = batchReadable.getSchema().numColumns();

        assertEquals(numRowsPerBatch.length, numBatches, "wrong number of batches");

        try (var reader = batchReadable.createRandomAccessReader()) {
            var batchStartRow = 0L;

            // Loop over batches
            for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) {
                var writtenBatch = reader.readRetained(batchIdx);
                var currentBatchNumRows = writtenBatch.length();
                assertEquals(numRowsPerBatch[batchIdx], currentBatchNumRows,
                    "wrong number of rows in batch " + batchIdx);

                // Loop over columns
                for (int col = 0; col < numColumns; col++) {
                    var writteData = writtenBatch.get(col);

                    // Loop over rows of the batch
                    for (int dataIdx = 0; dataIdx < currentBatchNumRows; dataIdx++) {
                        testData[col].assertData(writteData, dataIdx, batchStartRow + dataIdx);
                    }
                }
                batchStartRow += currentBatchNumRows;
                writtenBatch.release();
            }
        }
    }

    // ====== TEST DATA

    interface TestDataWriter {
        void writeTo(WriteAccess access, long rowIdx);
    }

    interface TestDataChecker {
        void assertData(NullableReadData data, int dataIdx, long rowIdx);
    }

    interface TestData extends TestDataWriter, TestDataChecker {
        DataSpecWithTraits getSpec();
    }

    private static int[] listForIdx(final long rowIdx) {
        return IntStream.range(0, (int)(rowIdx % 10)).toArray();
    }

    private static final class ObjectData {

        private static final ObjectSerializer<ObjectData> SERIALIZER = //
            (output, object) -> output.writeLong(object.m_val);

        private static final ObjectDeserializer<ObjectData> DESERIALIZER = //
            (input) -> new ObjectData(input.readLong());

        private long m_val;

        public ObjectData(final long val) {
            m_val = val;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(m_val);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof ObjectData o) {
                return o.m_val == this.m_val;
            }
            return false;
        }

        @Override
        public String toString() {
            return "ObjectData{" + m_val + "}";
        }
    }

    enum TestDataImpl implements TestData {

            INT(DataSpecs.INT, //
                (access, rowIdx) -> ((IntWriteAccess)access).setIntValue((int)rowIdx),
                (data, dataIdx, rowIdx) -> assertEquals((int)rowIdx, ((IntReadData)data).getInt(dataIdx),
                    wrongValueMessage(dataIdx, rowIdx))),

            STRING(DataSpecs.STRING, //
                (access, rowIdx) -> ((StringWriteAccess)access).setStringValue("str_" + rowIdx),
                (data, dataIdx, rowIdx) -> assertEquals("str_" + rowIdx, ((StringReadData)data).getString(dataIdx),
                    wrongValueMessage(dataIdx, rowIdx))),

            LIST_OF_INT(DataSpecs.LIST.of(DataSpecs.INT), //
                (access, rowIdx) -> {
                    var list = listForIdx(rowIdx);
                    var listAccess = (ListWriteAccess)access;
                    listAccess.create(list.length);
                    for (int i = 0; i < list.length; i++) {
                        listAccess.setWriteIndex(i);
                        ((IntWriteAccess)listAccess.getWriteAccess()).setIntValue(list[i]);
                    }
                }, //
                (data, dataIdx, rowIdx) -> {
                    var listData = (IntReadData)((ListReadData)data).createReadData(dataIdx);
                    var list = IntStream.range(0, listData.length()).map(i -> listData.getInt(i)).toArray();
                    assertArrayEquals(listForIdx(rowIdx), list, wrongValueMessage(dataIdx, rowIdx));

                } //
            ), STRUCT_OF_STRING(DataSpecs.STRUCT.of(DataSpecs.STRING, DataSpecs.STRING), //
                (access, rowIdx) -> {
                    var structAccess = (StructWriteAccess)access;
                    ((StringWriteAccess)structAccess.getWriteAccess(0)).setStringValue("A" + rowIdx);
                    ((StringWriteAccess)structAccess.getWriteAccess(1)).setStringValue("B" + rowIdx);
                }, //
                (data, dataIdx, rowIdx) -> {
                    var accessA = (StringReadData)((StructReadData)data).getReadDataAt(0);
                    var accessB = (StringReadData)((StructReadData)data).getReadDataAt(1);
                    assertEquals("A" + rowIdx, accessA.getString(dataIdx), wrongValueMessage(dataIdx, rowIdx));
                    assertEquals("B" + rowIdx, accessB.getString(dataIdx), wrongValueMessage(dataIdx, rowIdx));
                } //
            ),

            OBJECT(DataSpecs.VARBINARY, //
                (access, rowIdx) -> ((VarBinaryWriteAccess)access).setObject(new ObjectData(rowIdx),
                    ObjectData.SERIALIZER),
                (data, dataIdx, rowIdx) -> assertEquals(new ObjectData(rowIdx),
                    ((VarBinaryReadData)data).getObject(dataIdx, ObjectData.DESERIALIZER),
                    wrongValueMessage(dataIdx, rowIdx)));

        DataSpecWithTraits m_spec;

        private TestDataWriter m_writer;

        private TestDataChecker m_checker;

        TestDataImpl(final DataSpecWithTraits spec, final TestDataWriter writer, final TestDataChecker checker) {
            m_spec = spec;
            m_writer = writer;
            m_checker = checker;
        }

        @Override
        public DataSpecWithTraits getSpec() {
            return m_spec;
        }

        @Override
        public void writeTo(final WriteAccess access, final long rowIdx) {
            m_writer.writeTo(access, rowIdx);
        }

        @Override
        public void assertData(final NullableReadData data, final int dataIdx, final long rowIdx) {
            m_checker.assertData(data, dataIdx, rowIdx);
        }

        private static String wrongValueMessage(final int dataIdx, final long rowIdx) {
            return "wrong value at row " + rowIdx + ", data idx " + dataIdx;
        }
    }

    class FailingSerializeObjectData implements TestData {

        private static final ObjectSerializer<ObjectData> SERIALIZER = (output, object) -> {
            if (object.m_val >= 10) {
                throw new RuntimeException("This serializer is buggy");
            }
            output.writeLong(object.m_val);
        };

        @Override
        public void writeTo(final WriteAccess access, final long rowIdx) {
            ((VarBinaryWriteAccess)access).setObject(new ObjectData(rowIdx), FailingSerializeObjectData.SERIALIZER);
        }

        @Override
        public void assertData(final NullableReadData data, final int dataIdx, final long rowIdx) {
            throw new AssertionError("this method should not be called");
        }

        @Override
        public DataSpecWithTraits getSpec() {
            return DataSpecs.VARBINARY;
        }
    }
}
