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

import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.FloatData.FloatReadData;
import org.knime.core.columnar.data.FloatData.FloatWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ListData.ListDataSpec;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.ObjectData.GenericObjectDataSpec;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.columnar.data.StructData.StructDataSpec;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.columnar.testing.TestColumnStore;
import org.knime.core.columnar.testing.data.TestData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestColumnStoreUtils {

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

        int size() {
            return m_batches.size();
        }

    }

    private static final int DEF_NUM_COLUMNS = Types.values().length;

    private static final int DEF_NUM_BATCHES = 2;

    public static final int DEF_SIZE_OF_DATA = 2;

    public static final int DEF_SIZE_OF_TABLE = DEF_NUM_BATCHES * DEF_NUM_COLUMNS * DEF_SIZE_OF_DATA;

    private TestColumnStoreUtils() {
        // Utility class
    }

    private enum Types {
            BOOLEAN {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.booleanSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    ((BooleanWriteData)data).setBoolean(index, runningInt++ % 2 == 0); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    assertEquals(((BooleanReadData)refData).getBoolean(index),
                        ((BooleanReadData)readData).getBoolean(index));
                }
            },

            BYTE {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.byteSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    ((ByteWriteData)data).setByte(index, (byte)runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    assertEquals(((ByteReadData)refData).getByte(index), ((ByteReadData)readData).getByte(index));
                }
            },

            DOUBLE {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.doubleSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    ((DoubleWriteData)data).setDouble(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    assertEquals(((DoubleReadData)refData).getDouble(index),
                        ((DoubleReadData)readData).getDouble(index), 0d);
                }
            },

            DURATION {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.durationSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<Duration> objectData = ((ObjectWriteData<Duration>)data);
                    objectData.setObject(index, Duration.ofMillis(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<Duration> refObjectData = (ObjectReadData<Duration>)refData;
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<Duration> readObjectData = (ObjectReadData<Duration>)readData;
                    assertEquals(refObjectData.getObject(index), readObjectData.getObject(index));
                }
            },

            FLOAT {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.floatSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    ((FloatWriteData)data).setFloat(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    assertEquals(((FloatReadData)refData).getFloat(index), ((FloatReadData)readData).getFloat(index),
                        0f);
                }
            },

            INT {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.intSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    ((IntWriteData)data).setInt(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    assertEquals(((IntReadData)refData).getInt(index), ((IntReadData)readData).getInt(index));
                }
            },

            LIST {
                @Override
                ColumnDataSpec getSpec() {
                    return new ListDataSpec(ColumnDataSpec.intSpec());
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    final IntWriteData intData = ((ListWriteData)data).getWriteData(index, 1);
                    intData.setInt(0, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    final IntReadData refIntData = ((ListReadData)refData).getReadData(index);
                    final IntReadData readIntData = ((ListReadData)readData).getReadData(index);
                    assertEquals(refIntData.getInt(0), readIntData.getInt(0));
                }
            },

            LOCALDATE {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.localDateSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<LocalDate> objectData = ((ObjectWriteData<LocalDate>)data);
                    objectData.setObject(index, LocalDate.ofEpochDay(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<LocalDate> refObjectData = (ObjectReadData<LocalDate>)refData;
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<LocalDate> readObjectData = (ObjectReadData<LocalDate>)readData;
                    assertEquals(refObjectData.getObject(index), readObjectData.getObject(index));
                }
            },

            LOCALDATETIME {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.localDateTimeSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<LocalDateTime> objectData = ((ObjectWriteData<LocalDateTime>)data);
                    objectData.setObject(index,
                        LocalDateTime.of(LocalDate.ofEpochDay(runningInt++), LocalTime.ofNanoOfDay(runningInt++))); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<LocalDateTime> refObjectData = (ObjectReadData<LocalDateTime>)refData;
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<LocalDateTime> readObjectData = (ObjectReadData<LocalDateTime>)readData;
                    assertEquals(refObjectData.getObject(index), readObjectData.getObject(index));
                }
            },

            LOCALTIME {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.localTimeSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<LocalTime> objectData = ((ObjectWriteData<LocalTime>)data);
                    objectData.setObject(index, LocalTime.ofNanoOfDay(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<LocalTime> refObjectData = (ObjectReadData<LocalTime>)refData;
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<LocalTime> readObjectData = (ObjectReadData<LocalTime>)readData;
                    assertEquals(refObjectData.getObject(index), readObjectData.getObject(index));
                }
            },

            LONG {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.longSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    ((LongWriteData)data).setLong(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    assertEquals(((LongReadData)refData).getLong(index), ((LongReadData)readData).getLong(index));
                }
            },

            MISSING {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.intSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    ((IntWriteData)data).setMissing(index);
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    assertEquals(((IntReadData)refData).isMissing(index), ((IntReadData)readData).isMissing(index));
                }
            },

            OBJECT {
                @Override
                ColumnDataSpec getSpec() {
                    return new GenericObjectDataSpec<Integer>(null, false);
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<Integer> objectData = ((ObjectWriteData<Integer>)data);
                    objectData.setObject(index, Integer.valueOf(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<Integer> refObjectData = (ObjectReadData<Integer>)refData;
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<Integer> readObjectData = (ObjectReadData<Integer>)readData;
                    assertEquals(refObjectData.getObject(index), readObjectData.getObject(index));
                }
            },

            PERIOD {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.periodSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<Period> objectData = ((ObjectWriteData<Period>)data);
                    objectData.setObject(index, Period.ofDays(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<Period> refObjectData = (ObjectReadData<Period>)refData;
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<Period> readObjectData = (ObjectReadData<Period>)readData;
                    assertEquals(refObjectData.getObject(index), readObjectData.getObject(index));
                }
            },

            STRING {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.stringSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<String> objectData = ((ObjectWriteData<String>)data);
                    objectData.setObject(index, Integer.toString(runningInt++)); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<String> refObjectData = (ObjectReadData<String>)refData;
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<String> readObjectData = (ObjectReadData<String>)readData;
                    assertEquals(refObjectData.getObject(index), readObjectData.getObject(index));
                }
            },

            STRUCT {
                @Override
                ColumnDataSpec getSpec() {
                    return new StructDataSpec(ColumnDataSpec.intSpec());
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    final IntWriteData intData = ((StructWriteData)data).getWriteDataAt(0);
                    intData.setInt(index, runningInt++); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    final IntReadData refIntData = ((StructReadData)refData).getReadDataAt(0);
                    final IntReadData readIntData = ((StructReadData)readData).getReadDataAt(0);
                    assertEquals(refIntData.getInt(index), readIntData.getInt(index));
                }
            },

            VARBINARY {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.varBinarySpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    ((VarBinaryWriteData)data).setBytes(index, new byte[]{(byte)runningInt++}); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    assertArrayEquals(((VarBinaryReadData)refData).getBytes(index),
                        ((VarBinaryReadData)readData).getBytes(index));
                }
            },

            ZONEDDATETIME {
                @Override
                ColumnDataSpec getSpec() {
                    return ColumnDataSpec.zonedDateTimeSpec();
                }

                @Override
                void setData(final ColumnWriteData data, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectWriteData<ZonedDateTime> objectData = ((ObjectWriteData<ZonedDateTime>)data);
                    objectData.setObject(index, ZonedDateTime.of(LocalDate.ofEpochDay(runningInt++), // NOSONAR
                        LocalTime.ofNanoOfDay(runningInt++), ZoneId.of("Z"))); // NOSONAR
                }

                @Override
                void checkEquals(final ColumnReadData refData, final ColumnReadData readData, final int index) {
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<ZonedDateTime> refObjectData = (ObjectReadData<ZonedDateTime>)refData;
                    @SuppressWarnings("unchecked")
                    final ObjectReadData<ZonedDateTime> readObjectData = (ObjectReadData<ZonedDateTime>)readData;
                    assertEquals(refObjectData.getObject(index), readObjectData.getObject(index));
                }
            };

        abstract ColumnDataSpec getSpec();

        abstract void setData(ColumnWriteData data, int index);

        abstract void checkEquals(ColumnReadData refData, ColumnReadData readData, final int index);
    }

    private static Types indexToTypeT(final int index) {
        return Types.values()[index % Types.values().length];
    }

    public static ColumnStoreSchema createSchema(final int numColumns) {
        return new ColumnStoreSchema() {
            @Override
            public int getNumColumns() {
                return numColumns;
            }

            @Override
            public ColumnDataSpec getColumnDataSpec(final int index) {
                return indexToTypeT(index).getSpec();
            }
        };
    }

    private static ColumnStoreSchema createDefaultSchema() {
        return createSchema(DEF_NUM_COLUMNS);
    }

    public static TestColumnStore createDefaultTestColumnStore() {
        return TestColumnStore.create(createDefaultSchema());
    }

    private static ColumnWriteData[] createBatch(final ColumnStore store) {
        final WriteBatch batch = store.getFactory().create(DEF_SIZE_OF_DATA);
        final ColumnWriteData[] data = new ColumnWriteData[store.getSchema().getNumColumns()];

        for (int i = 0; i < store.getSchema().getNumColumns(); i++) {
            for (int j = 0; j < batch.capacity(); j++) {
                final ColumnWriteData data1 = batch.get(i);
                indexToTypeT(i).setData(data1, j);
                data[i] = data1;
            }
        }
        batch.close(batch.capacity());

        return data;
    }

    private static List<ColumnWriteData[]> createWriteTable(final ColumnStore store, final int numBatches) {
        return IntStream.range(0, numBatches).mapToObj(i -> createBatch(store)).collect(Collectors.toList());
    }

    public static TestDataTable createEmptyTestTable(final TestColumnStore store) {
        return new TestDataTable(createTestTable(store, 0));
    }

    public static TestDataTable createDefaultTestTable(final TestColumnStore store) {
        return new TestDataTable(createTestTable(store, DEF_NUM_BATCHES));
    }

    public static TestDataTable createDoubleSizedDefaultTestTable(final TestColumnStore store) {
        return new TestDataTable(createTestTable(store, 2 * DEF_NUM_BATCHES));
    }

    public static List<TestData[]> createTestTable(final TestColumnStore store, final int numBatches) {
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

    public static List<ColumnReadData[]> writeDefaultTable(final ColumnStore store) throws IOException {
        return writeTable(store, createWriteTable(store, DEF_NUM_BATCHES));
    }

    private static List<ColumnReadData[]> writeTable(final ColumnStore store, final List<ColumnWriteData[]> table)
        throws IOException {
        List<ColumnReadData[]> result = new ArrayList<>();
        try (final ColumnDataWriter writer = store.getWriter()) {
            for (WriteData[] writeBatch : table) {
                final ColumnReadData[] readBatch =
                    Arrays.stream(writeBatch).map(d -> d.close(d.capacity())).toArray(ColumnReadData[]::new);
                result.add(readBatch);
                writer.write(new DefaultReadBatch(readBatch, readBatch[0].length()));
            }
        }
        return result;
    }

    public static void writeTable(final ColumnStore store, final TestDataTable table) throws IOException {
        try (final ColumnDataWriter writer = store.getWriter()) {
            for (int i = 0; i < table.size(); i++) {
                final TestData[] batch = table.getBatch(i);
                final int length = batch[0].length();
                writer.write(new DefaultReadBatch(batch, length));
            }
        }
    }

    public static boolean tableInStore(final ColumnStore store, final TestDataTable table) throws IOException {
        try (final ColumnDataReader reader = store.createReader()) { // NOSONAR
        } catch (IllegalStateException e) { // NOSONAR
            return false;
        }
        try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) { // NOSONAR
        }
        return true;
    }

    public static TestDataTable readAndCompareTable(final ColumnReadStore store, final TestDataTable table)
        throws IOException {
        return readSelectionAndCompareTable(store, table, null);
    }

    @SuppressWarnings("resource")
    private static ColumnDataReader createReader(final ColumnReadStore store, final int[] indices) {
        return indices == null ? store.createReader()
            : store.createReader(new FilteredColumnSelection(store.getSchema().getNumColumns(), indices));
    }

    public static TestDataTable readSelectionAndCompareTable(final ColumnReadStore store, final TestDataTable table,
        final int... indices) throws IOException {

        try (final ColumnDataReader reader = createReader(store, indices)) {
            assertEquals(table.size(), reader.getNumBatches());

            final List<TestData[]> result = new ArrayList<>();
            for (int i = 0; i < reader.getNumBatches(); i++) {
                final TestData[] written = table.getBatch(i);
                final ReadBatch batch = reader.readRetained(i);
                final TestData[] data = new TestData[store.getSchema().getNumColumns()];

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

    public static void readAndCompareTable(final ColumnReadStore store, final List<ColumnReadData[]> table)
        throws IOException {
        readSelectionAndCompareTable(store, table, null);
    }

    public static void readSelectionAndCompareTable(final ColumnReadStore store, final List<ColumnReadData[]> table,
        final int... indices) throws IOException {

        try (final ColumnDataReader reader = createReader(store, indices)) {
            assertEquals(table.size(), reader.getNumBatches());

            for (int i = 0; i < reader.getNumBatches(); i++) {
                final ColumnReadData[] refBatch = table.get(i);
                final ReadBatch readBatch = reader.readRetained(i);

                assertEquals(refBatch.length, readBatch.getNumColumns());
                assertEquals(refBatch[0].length(), readBatch.length());
                final int[] indicesNonNull =
                    indices == null ? IntStream.range(0, store.getSchema().getNumColumns()).toArray() : indices;
                for (int j : indicesNonNull) {
                    final ColumnReadData refData = refBatch[j];
                    final ColumnReadData readData = readBatch.get(j);
                    compareData(refData, readData, j);
                }

                readBatch.release();
            }
        }
    }

    public static void releaseTable(final List<ColumnReadData[]> table) {
        for (ColumnReadData[] batch : table) {
            for (ColumnReadData data : batch) {
                data.release();
            }
        }
    }

    public static void readTwiceAndCompareTable(final ColumnReadStore store) throws IOException {
        try (final ColumnDataReader reader1 = store.createReader();
                final ColumnDataReader reader2 = store.createReader()) {
            assertEquals(reader1.getNumBatches(), reader2.getNumBatches());
            for (int i = 0; i < reader1.getNumBatches(); i++) {
                final ReadBatch batch1 = reader1.readRetained(i);
                final ReadBatch batch2 = reader2.readRetained(i);

                assertEquals(batch1.getNumColumns(), batch2.getNumColumns());
                assertEquals(batch1.length(), batch2.length());
                for (int j = 0; j < batch1.getNumColumns(); j++) {
                    final ColumnReadData data1 = batch1.get(j);
                    final ColumnReadData data2 = batch2.get(j);
                    compareData(data1, data2, j);
                }

                batch1.release();
                batch2.release();
            }
        }
    }

    private static void compareData(final ColumnReadData refData, final ColumnReadData readData, final int colIndex) {
        final int length = refData.length();
        assertEquals(length, readData.length());

        for (int k = 0; k < length; k++) {
            indexToTypeT(colIndex).checkEquals(refData, readData, k);
        }
    }

}
