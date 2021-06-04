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
 *   20 Aug 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.DurationData.DurationReadData;
import org.knime.core.columnar.data.DurationData.DurationWriteData;
import org.knime.core.columnar.data.FloatData.FloatReadData;
import org.knime.core.columnar.data.FloatData.FloatWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.LocalDateData.LocalDateReadData;
import org.knime.core.columnar.data.LocalDateData.LocalDateWriteData;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeReadData;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeWriteData;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeReadData;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeWriteData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.PeriodData.PeriodReadData;
import org.knime.core.columnar.data.PeriodData.PeriodWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeReadData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeWriteData;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.testing.TestBatchBuffer;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.columnar.testing.data.TestData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestBatchStoreUtils {

    private static int runningInt = 0;

    public static final class TestDataTable implements Closeable {

        private final List<TestData[]> m_batches;

        private TestDataTable(final List<TestData[]> batches) {
            m_batches = batches;
        }

        public TestData[] getBatch(final int index) {
            return m_batches.get(index);
        }

        @Override
        public void close() {
            for (TestData[] batch : m_batches) {
                for (TestData data : batch) {
                    if (data != null) {
                        data.release();
                    }
                }
            }
        }

        public int size() {
            return m_batches.size();
        }

    }

    private static final int DEF_NUM_COLUMNS = Types.values().length;

    private static final int DEF_NUM_BATCHES = 2;

    public static final int DEF_BATCH_LENGTH = 2;

    public static final int DEF_SIZE_OF_TABLE = DEF_NUM_BATCHES * DEF_NUM_COLUMNS * DEF_BATCH_LENGTH;

    private TestBatchStoreUtils() {
        // Utility class
    }

    private enum Types {
            BOOLEAN {
                @Override
                DataSpec getSpec() {
                    return DataSpec.booleanSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    ((BooleanWriteData)data).setBoolean(index, runningInt++ % 2 == 0); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    assertEquals(((BooleanReadData)refData).getBoolean(index),
                        ((BooleanReadData)readData).getBoolean(index));
                }
            },

            BYTE {
                @Override
                DataSpec getSpec() {
                    return DataSpec.byteSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    ((ByteWriteData)data).setByte(index, (byte)runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    assertEquals(((ByteReadData)refData).getByte(index), ((ByteReadData)readData).getByte(index));
                }
            },

            DOUBLE {
                @Override
                DataSpec getSpec() {
                    return DataSpec.doubleSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    ((DoubleWriteData)data).setDouble(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    assertEquals(((DoubleReadData)refData).getDouble(index),
                        ((DoubleReadData)readData).getDouble(index), 0d);
                }
            },

            DURATION {
                @Override
                DataSpec getSpec() {
                    return DataSpec.durationSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final DurationWriteData objectData = ((DurationWriteData)data);
                    objectData.setDuration(index, Duration.ofMillis(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final DurationReadData refObjectData = (DurationReadData)refData;
                    final DurationReadData readObjectData = (DurationReadData)readData;
                    assertEquals(refObjectData.getDuration(index), readObjectData.getDuration(index));
                }
            },

            FLOAT {
                @Override
                DataSpec getSpec() {
                    return DataSpec.floatSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    ((FloatWriteData)data).setFloat(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    assertEquals(((FloatReadData)refData).getFloat(index), ((FloatReadData)readData).getFloat(index),
                        0f);
                }
            },

            INT {
                @Override
                DataSpec getSpec() {
                    return DataSpec.intSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    ((IntWriteData)data).setInt(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    assertEquals(((IntReadData)refData).getInt(index), ((IntReadData)readData).getInt(index));
                }
            },

            LIST {
                @Override
                DataSpec getSpec() {
                    return new ListDataSpec(DataSpec.intSpec());
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final IntWriteData intData = ((ListWriteData)data).createWriteData(index, 1);
                    intData.setInt(0, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final IntReadData refIntData = ((ListReadData)refData).createReadData(index);
                    final IntReadData readIntData = ((ListReadData)readData).createReadData(index);
                    assertEquals(refIntData.getInt(0), readIntData.getInt(0));
                }
            },

            LOCALDATE {
                @Override
                DataSpec getSpec() {
                    return DataSpec.localDateSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final LocalDateWriteData objectData = ((LocalDateWriteData)data);
                    objectData.setLocalDate(index, LocalDate.ofEpochDay(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final LocalDateReadData refObjectData = (LocalDateReadData)refData;
                    final LocalDateReadData readObjectData = (LocalDateReadData)readData;
                    assertEquals(refObjectData.getLocalDate(index), readObjectData.getLocalDate(index));
                }
            },

            LOCALDATETIME {
                @Override
                DataSpec getSpec() {
                    return DataSpec.localDateTimeSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final LocalDateTimeWriteData objectData = ((LocalDateTimeWriteData)data);
                    objectData.setLocalDateTime(index,
                        LocalDateTime.of(LocalDate.ofEpochDay(runningInt++), LocalTime.ofNanoOfDay(runningInt++))); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final LocalDateTimeReadData refObjectData = (LocalDateTimeReadData)refData;
                    final LocalDateTimeReadData readObjectData = (LocalDateTimeReadData)readData;
                    assertEquals(refObjectData.getLocalDateTime(index), readObjectData.getLocalDateTime(index));
                }
            },

            LOCALTIME {
                @Override
                DataSpec getSpec() {
                    return DataSpec.localTimeSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final LocalTimeWriteData objectData = ((LocalTimeWriteData)data);
                    objectData.setLocalTime(index, LocalTime.ofNanoOfDay(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final LocalTimeReadData refObjectData = (LocalTimeReadData)refData;
                    final LocalTimeReadData readObjectData = (LocalTimeReadData)readData;
                    assertEquals(refObjectData.getLocalTime(index), readObjectData.getLocalTime(index));
                }
            },

            LONG {
                @Override
                DataSpec getSpec() {
                    return DataSpec.longSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    ((LongWriteData)data).setLong(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    assertEquals(((LongReadData)refData).getLong(index), ((LongReadData)readData).getLong(index));
                }
            },

            MISSING {
                @Override
                DataSpec getSpec() {
                    return DataSpec.intSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    ((IntWriteData)data).setMissing(index);
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    assertEquals(((IntReadData)refData).isMissing(index), ((IntReadData)readData).isMissing(index));
                }
            },

            PERIOD {
                @Override
                DataSpec getSpec() {
                    return DataSpec.periodSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final PeriodWriteData objectData = ((PeriodWriteData)data);
                    objectData.setPeriod(index, Period.ofDays(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final PeriodReadData refObjectData = (PeriodReadData)refData;
                    final PeriodReadData readObjectData = (PeriodReadData)readData;
                    assertEquals(refObjectData.getPeriod(index), readObjectData.getPeriod(index));
                }
            },

            STRING {
                @Override
                DataSpec getSpec() {
                    return DataSpec.stringSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final StringWriteData objectData = ((StringWriteData)data);
                    objectData.setString(index, Integer.toString(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final StringReadData refObjectData = (StringReadData)refData;
                    final StringReadData readObjectData = (StringReadData)readData;
                    assertEquals(refObjectData.getString(index), readObjectData.getString(index));
                }
            },

            STRUCT {
                @Override
                DataSpec getSpec() {
                    return new StructDataSpec(DataSpec.intSpec());
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final IntWriteData intData = ((StructWriteData)data).getWriteDataAt(0);
                    intData.setInt(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final IntReadData refIntData = ((StructReadData)refData).getReadDataAt(0);
                    final IntReadData readIntData = ((StructReadData)readData).getReadDataAt(0);
                    assertEquals(refIntData.getInt(index), readIntData.getInt(index));
                }
            },

            VARBINARY {
                @Override
                DataSpec getSpec() {
                    return DataSpec.varBinarySpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    ((VarBinaryWriteData)data).setBytes(index, new byte[]{(byte)runningInt++}); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    assertArrayEquals(((VarBinaryReadData)refData).getBytes(index),
                        ((VarBinaryReadData)readData).getBytes(index));
                }
            },

            ZONEDDATETIME {
                @Override
                DataSpec getSpec() {
                    return DataSpec.zonedDateTimeSpec();
                }

                @Override
                void setData(final NullableWriteData data, final int index) {
                    final ZonedDateTimeWriteData objectData = (ZonedDateTimeWriteData)data;
                    objectData.setZonedDateTime(index, ZonedDateTime.of(LocalDate.ofEpochDay(runningInt++), // NOSONAR
                        LocalTime.ofNanoOfDay(runningInt++), ZoneId.of("Z"))); // NOSONAR
                }

                @Override
                void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
                    final ZonedDateTimeReadData refObjectData = (ZonedDateTimeReadData)refData;
                    final ZonedDateTimeReadData readObjectData = (ZonedDateTimeReadData)readData;
                    assertEquals(refObjectData.getZonedDateTime(index), readObjectData.getZonedDateTime(index));
                }
            };

        abstract DataSpec getSpec();

        abstract void setData(NullableWriteData data, int index);

        abstract void checkEquals(NullableReadData refData, NullableReadData readData, final int index);
    }

    private static Types indexToTypeT(final int index) {
        return Types.values()[index % Types.values().length];
    }

    public static ColumnarSchema createSchema(final int numColumns) {
        return new ColumnarSchema() {
            @Override
            public int numColumns() {
                return numColumns;
            }

            @Override
            public DataSpec getSpec(final int index) {
                return indexToTypeT(index).getSpec();
            }
        };
    }

    private static ColumnarSchema createDefaultSchema() {
        return createSchema(DEF_NUM_COLUMNS);
    }

    public static TestBatchStore createDefaultTestColumnStore() {
        return TestBatchStore.create(createDefaultSchema());
    }

    public static TestBatchBuffer createDefaultTestBatchBuffer() {
        return TestBatchBuffer.create(createDefaultSchema());
    }

    private static NullableWriteData[] createBatch(final BatchWritable store) {
        @SuppressWarnings("resource")
        final WriteBatch batch = store.getWriter().create(DEF_BATCH_LENGTH);
        final NullableWriteData[] data = new NullableWriteData[store.getSchema().numColumns()];

        for (int i = 0; i < store.getSchema().numColumns(); i++) {
            for (int j = 0; j < batch.capacity(); j++) {
                final NullableWriteData data1 = batch.get(i);
                indexToTypeT(i).setData(data1, j);
                data[i] = data1;
            }
        }
        batch.close(batch.capacity());

        return data;
    }

    private static List<NullableWriteData[]> createWriteTable(final BatchWritable store, final int numBatches) {
        return IntStream.range(0, numBatches).mapToObj(i -> createBatch(store)).collect(Collectors.toList());
    }

    public static TestDataTable createEmptyTestTable(final TestBatchStore store) {
        return new TestDataTable(createTestTable(store, 0));
    }

    public static TestDataTable createDefaultTestTable(final TestBatchStore store) {
        return new TestDataTable(createTestTable(store, DEF_NUM_BATCHES));
    }

    public static TestDataTable createDoubleSizedDefaultTestTable(final TestBatchStore store) {
        return new TestDataTable(createTestTable(store, 2 * DEF_NUM_BATCHES));
    }

    public static List<TestData[]> createTestTable(final TestBatchStore store, final int numBatches) {
        return IntStream.range(0, numBatches)
            .mapToObj(i -> Arrays.stream(createBatch(store)).map(d -> (TestData)d).toArray(TestData[]::new))
            .collect(Collectors.toList());
    }

    private static int checkRefs(final TestData[] batch) {
        if (batch.length == 0) {
            return 0;
        }
        final int refs = batch[0].getRefs();
        for (final TestData data : batch) {
            assertEquals(refs, data.getRefs());
        }
        return refs;
    }

    public static int checkRefs(final TestDataTable table) {
        if (table.size() == 0) {
            return 0;
        }
        final int refs = checkRefs(table.getBatch(0));
        for (int i = 0; i < table.size(); i++) {
            assertEquals(refs, checkRefs(table.getBatch(i)));
        }
        return refs;
    }

    public static List<NullableReadData[]> writeDefaultTable(final BatchWritable store) throws IOException {
        return writeTable(store, createWriteTable(store, DEF_NUM_BATCHES));
    }

    private static List<NullableReadData[]> writeTable(final BatchWritable store, final List<NullableWriteData[]> table)
        throws IOException {
        List<NullableReadData[]> result = new ArrayList<>();
        try (final BatchWriter writer = store.getWriter()) {
            for (WriteData[] writeBatch : table) {
                final NullableReadData[] readBatch =
                    Arrays.stream(writeBatch).map(d -> d.close(d.capacity())).toArray(NullableReadData[]::new);
                result.add(readBatch);
                writer.write(new DefaultReadBatch(readBatch));
            }
        }
        return result;
    }

    public static void writeTable(final BatchWritable store, final TestDataTable table) throws IOException {
        try (final BatchWriter writer = store.getWriter()) {
            for (int i = 0; i < table.size(); i++) {
                final TestData[] batch = table.getBatch(i);
                writer.write(new DefaultReadBatch(batch));
            }
        }
    }

    public static boolean tableInStore(final RandomAccessBatchReadable store, final TestDataTable table)
        throws IOException {
        try (final RandomAccessBatchReader reader = store.createRandomAccessReader()) { // NOSONAR
        } catch (IllegalStateException e) { // NOSONAR
            return false;
        }
        try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) { // NOSONAR
        }
        return true;
    }

    public static TestDataTable readAndCompareTable(final RandomAccessBatchReadable store, final TestDataTable table)
        throws IOException {
        return readSelectionAndCompareTable(store, table, null);
    }

    @SuppressWarnings("resource")
    private static RandomAccessBatchReader createReader(final RandomAccessBatchReadable store, final int[] indices) {
        return indices == null ? store.createRandomAccessReader()
            : store.createRandomAccessReader(new FilteredColumnSelection(store.getSchema().numColumns(), indices));
    }

    public static TestDataTable readSelectionAndCompareTable(final RandomAccessBatchReadable store,
        final TestDataTable table, final int... indices) throws IOException {

        try (final RandomAccessBatchReader reader = createReader(store, indices)) {
            final List<TestData[]> result = new ArrayList<>();
            for (int i = 0; i < table.size(); i++) {
                final TestData[] written = table.getBatch(i);
                final ReadBatch batch = reader.readRetained(i);
                final TestData[] data = new TestData[store.getSchema().numColumns()];

                assertEquals(written.length, data.length);

                if (indices == null) {
                    for (int j = 0; j < written.length; j++) { // NOSONAR
                        data[j] = (TestData)batch.get(j);
                        assertArrayEquals(written[j].get(), data[j].get());
                    }
                } else {
                    for (int j : indices) { // NOSONAR
                        data[j] = (TestData)batch.get(j);
                        assertArrayEquals(written[j].get(), data[j].get());
                    }
                }

                result.add(data);
            }
            return new TestDataTable(result);
        }
    }

    public static void readAndCompareTable(final RandomAccessBatchReadable store, final List<NullableReadData[]> table)
        throws IOException {
        readSelectionAndCompareTable(store, table, null);
    }

    public static void readSelectionAndCompareTable(final RandomAccessBatchReadable store,
        final List<NullableReadData[]> table, final int... indices) throws IOException {

        try (final RandomAccessBatchReader reader = createReader(store, indices)) {
            for (int i = 0; i < table.size(); i++) {
                final NullableReadData[] refBatch = table.get(i);
                final ReadBatch readBatch = reader.readRetained(i);

                assertEquals(refBatch.length, readBatch.size());
                assertEquals(refBatch[0].length(), readBatch.length());
                final int[] indicesNonNull =
                    indices == null ? IntStream.range(0, store.getSchema().numColumns()).toArray() : indices;
                for (int j : indicesNonNull) {
                    final NullableReadData refData = refBatch[j];
                    final NullableReadData readData = readBatch.get(j);
                    compareData(refData, readData, j);
                }

                readBatch.release();
            }
        }
    }

    public static void releaseTable(final List<NullableReadData[]> table) {
        for (NullableReadData[] batch : table) {
            for (NullableReadData data : batch) {
                data.release();
            }
        }
    }

    public static void readTwiceAndCompareTable(final RandomAccessBatchReadable store, final int numBatches)
        throws IOException {
        try (final RandomAccessBatchReader reader1 = store.createRandomAccessReader();
                final RandomAccessBatchReader reader2 = store.createRandomAccessReader()) {
            for (int i = 0; i < numBatches; i++) {
                final ReadBatch batch1 = reader1.readRetained(i);
                final ReadBatch batch2 = reader2.readRetained(i);

                assertEquals(batch1.size(), batch2.size());
                assertEquals(batch1.length(), batch2.length());
                for (int j = 0; j < batch1.size(); j++) {
                    final NullableReadData data1 = batch1.get(j);
                    final NullableReadData data2 = batch2.get(j);
                    compareData(data1, data2, j);
                }

                batch1.release();
                batch2.release();
            }
        }
    }

    private static void compareData(final NullableReadData refData, final NullableReadData readData,
        final int colIndex) {
        final int length = refData.length();
        assertEquals(length, readData.length());

        for (int k = 0; k < length; k++) {
            indexToTypeT(colIndex).checkEquals(refData, readData, k);
        }
    }

}
