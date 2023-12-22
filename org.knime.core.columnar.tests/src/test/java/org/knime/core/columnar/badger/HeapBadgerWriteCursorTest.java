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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
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
 * @author chaubold
 */
class HeapBadgerWriteCursorTest {

    // Tests (always check that the heap cache is filled)
    // - One batch that is not full
    // - One batch that just enough - would switch to new batch with next data but should not
    // - Two batches - just switched to next data by one row because of max rows
    // - Two batches - just switched to next data by one row because of max size
    // - 4 batches
    // - Data so big that one row already surpasses the max size
    // - Settings such that one row of fixed data would surpass the max size
    // TODO we should also test edge cases of the buffer configuration

    @Test
    @DisplayName("no batching - int|string")
    void testSingleBatchStringInt() throws IOException {
        runFillAndCheckHeapBadgerTest( //
            25, // num rows
            26, // max num rows per batch
            Integer.MAX_VALUE, // max batch size in bytes
            new int[]{25}, // expected num rows
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
            TestDataImpl.INT, TestDataImpl.STRING // test data
        );
    }

    @ParameterizedTest
    @EnumSource(value = TestDataImpl.class)
    void testOneBatchWithTypes(final TestData data) throws IOException {
        runFillAndCheckHeapBadgerTest(10, 20, Integer.MAX_VALUE, new int[]{10}, data);
    }

    // ====== TEST UTILITIES

    private static void runFillAndCheckHeapBadgerTest( //
        final int numRows, //
        final int maxNumRowsPerBatch, //
        final int maxBatchSizeInBytes, //
        final int[] expectedNumRows, //
        final TestData... testData //
    ) throws IOException {
        try (TestBatchStore batchStore = createTestStore(testData)) {
            var badger = new HeapBadger(batchStore, maxNumRowsPerBatch, maxBatchSizeInBytes);
            writeToHeapBadger(badger, testData, numRows);

            // TODO assert the heap cache
            assertWrittenData(testData, expectedNumRows, batchStore);
        }
    }

    /** Create a new batch store that can store the given test data */
    private static TestBatchStore createTestStore(final TestData[] data) {
        var schema = ColumnarSchema.of(Arrays.stream(data).map(d -> d.getSpec()).toArray(DataSpecWithTraits[]::new));
        return TestBatchStore.create(schema);
    }

    /** Write some test data to the HeapBadger */
    private static void writeToHeapBadger(final HeapBadger badger, final TestData[] data, final long numRows)
        throws IOException {
        try (WriteCursor<WriteAccessRow> cursor = badger.getWriteCursor()) {
            for (long rowIdx = 0; rowIdx < numRows; rowIdx++) {
                cursor.forward();
                for (int col = 0; col < data.length; col++) {
                    data[col].writeTo(cursor.access().getWriteAccess(col), rowIdx);
                }
            }
            cursor.flush();
        }
    }

    /** Check the written data in the underlying store */
    private static void assertWrittenData(final TestData[] testData, final int[] numRowsPerBatch,
        final TestBatchStore batchStore) throws IOException {
        var numBatches = batchStore.numBatches();
        var numColumns = batchStore.getSchema().numColumns();

        assertEquals(numRowsPerBatch.length, numBatches, "wrong number of batches");

        try (var reader = batchStore.createRandomAccessReader()) {
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
}
