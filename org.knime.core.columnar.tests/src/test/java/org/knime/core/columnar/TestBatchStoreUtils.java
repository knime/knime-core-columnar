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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.DataIndex;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.columnar.data.DecoratingData;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.FloatData.FloatReadData;
import org.knime.core.columnar.data.FloatData.FloatWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictDecoder;
import org.knime.core.columnar.data.dictencoding.DictElementCache;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.testing.TestBatchBuffer;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.columnar.testing.data.TestData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DefaultListDataTraits;
import org.knime.core.table.schema.traits.DefaultStructDataTraits;

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

    public static final int DEF_NUM_COLUMNS = Types.values().length;

    public static final int DEF_NUM_BATCHES = 2;

    public static final int DEF_BATCH_LENGTH = 2;

    private static final int DEF_NUM_DATAS = Stream.of(Types.values())//
            .mapToInt(Types::getNumDatas)//
            .sum();

    // a single column may contain multiple datas therefore we need to use DEF_NUM_DATAS to calculate the actual
    // table size
    // NOTE: TestData#sizeOf returns the number of elements in a data object, not the actual size in bytes
    public static final int DEF_SIZE_OF_TABLE = DEF_NUM_BATCHES * DEF_NUM_DATAS * DEF_BATCH_LENGTH;


    private TestBatchStoreUtils() {
        // Utility class
    }

    private enum Types {
            BOOLEAN(//
                DataSpec.booleanSpec(),//
                DefaultDataTraits.EMPTY,//
                (data, index) -> {
                    ((BooleanWriteData)data).setBoolean(index, runningInt++ % 2 == 0); // NOSONAR
                    return data;
                },
                (refData, readData, index) -> assertEquals(((BooleanReadData)refData).getBoolean(index),
                    ((BooleanReadData)readData).getBoolean(index)),//
                1),

            BYTE(//
                DataSpec.byteSpec(),//
                DefaultDataTraits.EMPTY,//
                (data, index) -> {
                    ((ByteWriteData)data).setByte(index, (byte)runningInt++); // NOSONAR
                    return data;
                },//
                (e, a, i) -> assertEquals(((ByteReadData)e).getByte(i), ((ByteReadData)a).getByte(i)),//
                1),

            DOUBLE(
                DataSpec.doubleSpec(),//
                DefaultDataTraits.EMPTY,//
                (data, index) -> {
                    ((DoubleWriteData)data).setDouble(index, runningInt++); // NOSONAR
                    return data;
                },//
                (e, a, i) -> assertEquals(((DoubleReadData)e).getDouble(i), ((DoubleReadData)a).getDouble(i), 0d),//
                1),

            FLOAT(//
                DataSpec.floatSpec(),//
                DefaultDataTraits.EMPTY,//
                (data, index) -> {
                    ((FloatWriteData)data).setFloat(index, runningInt++); // NOSONAR
                    return data;
                },//
                (e, a, i) -> assertEquals(((FloatReadData)e).getFloat(i), ((FloatReadData)a).getFloat(i), 0f),//
                1),

            INT(//
                DataSpec.intSpec(),//
                DefaultDataTraits.EMPTY,//
                (data, index) -> {
                    ((IntWriteData)data).setInt(index, runningInt++); // NOSONAR
                    return data;
                },//
                (e, a, i) -> assertEquals(((IntReadData)e).getInt(i), ((IntReadData)a).getInt(i)),//
                1),

            LIST(//
                new ListDataSpec(DataSpec.intSpec()),//
                new DefaultListDataTraits(new DataTrait[0], DefaultDataTraits.EMPTY),//
                (data, index) -> {
                    final IntWriteData intData = ((ListWriteData)data).createWriteData(index, 1);
                    intData.setInt(0, runningInt++); // NOSONAR
                    return data;
                },//
                (refData, readData, index) -> {
                    final IntReadData refIntData = ((ListReadData)refData).createReadData(index);
                    final IntReadData readIntData = ((ListReadData)readData).createReadData(index);
                    assertEquals(refIntData.getInt(0), readIntData.getInt(0));
                },//
                1),

            LONG(//
                DataSpec.longSpec(),//
                DefaultDataTraits.EMPTY,//
                (data, index) -> {
                    ((LongWriteData)data).setLong(index, runningInt++); // NOSONAR
                    return data;
                },//
                (e, a, i) -> assertEquals(((LongReadData)e).getLong(i), ((LongReadData)a).getLong(i)),//
                1),

            MISSING(//
                DataSpec.intSpec(),
                DefaultDataTraits.EMPTY,
                (data, index) -> {
                    ((IntWriteData)data).setMissing(index);
                    return data;
                },
                (e, a, i) -> assertEquals(((IntReadData)e).isMissing(i), ((IntReadData)a).isMissing(i)),//
                1),

            STRING(//
                DataSpec.stringSpec(), //
                DefaultDataTraits.EMPTY, //
                (data, index) -> {
                    final StringWriteData objectData = ((StringWriteData)data);
                    objectData.setString(index, Integer.toString(runningInt++)); // NOSONAR
                    return data;
                }, //
                TestBatchStoreUtils::checkEqualsString,//
                1),

            DICTENCODEDSTRING(//
                DataSpec.stringSpec(),//
                new DefaultDataTraits(new DictEncodingTrait()),//
                TestBatchStoreUtils::setDictEncodedStringData,//
                TestBatchStoreUtils::checkEqualsString,//
                2),

            DICTENCODEDSTRING_INTKEY(//
                DataSpec.stringSpec(),//
                new DefaultDataTraits(new DictEncodingTrait(KeyType.INT_KEY)),//
                TestBatchStoreUtils::setDictEncodedStringData,//
                TestBatchStoreUtils::checkEqualsString,
                2),

            DICTENCODEDSTRING_BYTEKEY(//
                DataSpec.stringSpec(),//
                new DefaultDataTraits(new DictEncodingTrait(KeyType.BYTE_KEY)),//
                TestBatchStoreUtils::setDictEncodedStringData,//
                TestBatchStoreUtils::checkEqualsString,//
                2),

            STRUCT(//
                new StructDataSpec(DataSpec.intSpec()),//
                new DefaultStructDataTraits(new DataTrait[0], DefaultDataTraits.EMPTY),//
                TestBatchStoreUtils::setStructData,//
                TestBatchStoreUtils::checkEqualsStructData,//
                1),

            VARBINARY(//
                DataSpec.varBinarySpec(), //
                DefaultDataTraits.EMPTY, //
                TestBatchStoreUtils::setVarBinaryData, //
                TestBatchStoreUtils::checkEqualsVarBinary,//
                1),

            DICTENCODEDVARBINARY (//
                DataSpec.varBinarySpec(), //
                new DefaultDataTraits(new DictEncodingTrait()), //
                TestBatchStoreUtils::setDictEncodedVarBinaryData, //
                TestBatchStoreUtils::checkEqualsVarBinary,//
                2),

            DICTENCODEDVARBINARY_INTKEY (//
                DataSpec.varBinarySpec(), //
                new DefaultDataTraits(new DictEncodingTrait(KeyType.INT_KEY)), //
                TestBatchStoreUtils::setDictEncodedVarBinaryData, //
                TestBatchStoreUtils::checkEqualsVarBinary,//
                2),

            DICTENCODEDVARBINARY_BYTEKEY(//
                DataSpec.varBinarySpec(), //
                new DefaultDataTraits(new DictEncodingTrait(KeyType.BYTE_KEY)), //
                TestBatchStoreUtils::setDictEncodedVarBinaryData, //
                TestBatchStoreUtils::checkEqualsVarBinary,//
                2),

            STRUCT_OF_DICTENCODED_VARBINARY(//
                new StructDataSpec(DataSpec.varBinarySpec()), //
                new DefaultStructDataTraits(new DataTrait[0], new DefaultDataTraits(new DictEncodingTrait())), //
                TestBatchStoreUtils::setStructOfDictencodedVarBinaryData,//
                TestBatchStoreUtils::checkEqualsStructOfDictencodedVarBinaryData,//
                2),

            LIST_OF_STRUCT_OF_DICTENCODED_VARBINARY(//
                new ListDataSpec(new StructDataSpec(DataSpec.varBinarySpec())), //
                new DefaultListDataTraits(//
                    new DataTrait[0], //
                    new DefaultStructDataTraits(//
                        new DataTrait[0], //
                        new DefaultDataTraits(new DictEncodingTrait())//
                    )//
                ), //
                TestBatchStoreUtils::setListOfStructOfDictencodedVarBinaryData,//
                TestBatchStoreUtils::checkEqualsListOfStructOfDictencodedVarBinaryData,//
                2)
            ;

        private final DataTraits m_traits;

        private final DataSpec m_spec;

        private final DataSetter m_dataSetter;

        private final EqualsChecker m_equalsChecker;

        private final int m_numDatas;

        private Types(final DataSpec spec, final DataTraits traits, final DataSetter dataSetter,
            final EqualsChecker equalsChecker, final int numDatas) {
            m_spec = spec;
            m_traits = traits;
            m_dataSetter = dataSetter;
            m_equalsChecker = equalsChecker;
            m_numDatas = numDatas;
        }

        DataSpec getSpec() {
            return m_spec;
        }

        DataTraits getTraits() {
            return m_traits;
        }

        NullableWriteData setData(final NullableWriteData data, final int index) {
            return m_dataSetter.setData(data, index);
        }

        void checkEquals(final NullableReadData refData, final NullableReadData readData, final int index) {
            m_equalsChecker.checkEquals(refData, readData, index);
        }

        int getNumDatas() {
            return m_numDatas;
        }
    }

    interface DataSetter {
        NullableWriteData setData(final NullableWriteData data, int index);
    }

    interface EqualsChecker {
        void checkEquals(NullableReadData refData, NullableReadData readData, final int index);
    }

    private static Types indexToTypeT(final int index) {
        return Types.values()[index % Types.values().length];
    }

    public static ColumnarSchema createSchema(final int numColumns) {
        final List<DataSpec> columnSpecs =
            IntStream.range(0, numColumns).mapToObj(i -> indexToTypeT(i).getSpec()).collect(Collectors.toList());
        final List<DataTraits> columnTraits =
            IntStream.range(0, numColumns).mapToObj(i -> indexToTypeT(i).getTraits()).collect(Collectors.toList());
        return new DefaultColumnarSchema(columnSpecs, columnTraits);
    }

    public static ColumnarSchema createDefaultSchema() {
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
                data[i] = DecoratingData.unpack(indexToTypeT(i).setData(data1, j));
            }
        }

        return data;
    }

    private static List<NullableWriteData[]> createWriteTable(final BatchWritable store, final int numBatches) {
        return IntStream.range(0, numBatches)//
                .mapToObj(i -> createBatch(store))//
                .collect(Collectors.toList());
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

    private static NullableWriteData setDictEncodedStringData(final NullableWriteData data, final int index) {
        final StringWriteData objectData = ((StringWriteData)data);
        final var str = index % 2 == 0 ? Integer.toString(runningInt++) : "foo";
        objectData.setString(index, str);
        return DecoratingData.unpack(data);
    }

    private static NullableWriteData setDictEncodedVarBinaryData(final NullableWriteData data, final int index) {
        setVarBinaryData(data, index);
        return DecoratingData.unpack(data);
    }

    private static NullableWriteData setVarBinaryData(final NullableWriteData data, final int index) {
        ((VarBinaryWriteData)data).setBytes(index, new byte[]{(byte)runningInt++}); // NOSONAR
        return data;
    }

    private static NullableWriteData setStructData(final NullableWriteData data, final int index) {
        final IntWriteData intData = ((StructWriteData)data).getWriteDataAt(0);
        intData.setInt(index, runningInt++); // NOSONAR
        return data;
    }

    private static NullableWriteData setStructOfDictencodedVarBinaryData(final NullableWriteData data, final int index) {
        final VarBinaryWriteData varBinaryData = ((StructWriteData)data).getWriteDataAt(0);
        setVarBinaryData(varBinaryData, index);
        return data;
    }

    private static NullableWriteData setListOfStructOfDictencodedVarBinaryData(final NullableWriteData data,
        final int index) {
        StructWriteData structData = ((ListWriteData)data).createWriteData(index, 1);
        setStructOfDictencodedVarBinaryData(structData, 0);
        return data;
    }


    private static void checkEqualsVarBinary(final NullableReadData refData, final NullableReadData readData,
        final int index) {
        assertArrayEquals(((VarBinaryReadData)refData).getBytes(index), ((VarBinaryReadData)readData).getBytes(index));
    }

    private static void checkEqualsString(final NullableReadData refData, final NullableReadData readData,
        final int index) {
        final StringReadData refObjectData = (StringReadData)refData;
        final StringReadData readObjectData = (StringReadData)readData;
        final var ref = refObjectData.getString(index);
        final var read = readObjectData.getString(index);
        assertEquals(ref, read);
    }

    private static void checkEqualsStructData(final NullableReadData refData, final NullableReadData readData,
        final int index) {
        final IntReadData refIntData = ((StructReadData)refData).getReadDataAt(0);
        final IntReadData readIntData = ((StructReadData)readData).getReadDataAt(0);
        assertEquals(refIntData.getInt(index), readIntData.getInt(index));
    }

    private static void checkEqualsStructOfDictencodedVarBinaryData(final NullableReadData expected,
        final NullableReadData actual, final int index) {
        final VarBinaryReadData expectedVarBinary = ((StructReadData)expected).getReadDataAt(0);
        final VarBinaryReadData actualVarBinary = ((StructReadData)actual).getReadDataAt(0);
        checkEqualsVarBinary(expectedVarBinary, actualVarBinary, index);
    }

    private static void checkEqualsListOfStructOfDictencodedVarBinaryData(final NullableReadData expected,
        final NullableReadData actual, final int index) {
        final StructReadData expectedStruct = ((ListReadData)expected).createReadData(index);
        final StructReadData actualStruct = ((ListReadData)actual).createReadData(index);
        checkEqualsStructOfDictencodedVarBinaryData(expectedStruct, actualStruct, 0);
    }

    public static NullableReadData[] wrapDictEncodedData(final NullableReadData[] data, final DictElementCache cache,
        final ColumnarSchema schema) {
        NullableReadData[] out = new NullableReadData[data.length];
        final var dictDecoder = new DictDecoder(cache);
        for (int i = 0; i < out.length; i++) {
            out[i] = dictDecoder.decode(DataIndex.createColumnIndex(i), data[i], schema.getSpec(i), schema.getTraits(i));
        }
        return out;
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
                final var data = table.getBatch(i);
                // Retain the data to mimic the behavior of readRetained
                Stream.of(data).forEach(ReferencedData::retain);
                var batch = new DefaultReadBatch(data);
                writer.write(batch);
                batch.release();
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

                final var loopIndices = indices != null ? indices : IntStream.range(0, written.length).toArray();
                for (int j : loopIndices) { // NOSONAR
                    var d = DecoratingData.unpack(batch.get(j));
                    // TestDataTable#close releases the data, hence we need to retain it here because
                    // it may come from the cache
                    d.retain();
                    data[j] = (TestData)d;
                    assertArrayEquals(written[j].get(), data[j].get());
                }

                result.add(data);
                batch.release();
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

                assertEquals(refBatch.length, readBatch.numData());
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

                assertEquals(batch1.numData(), batch2.numData());
                assertEquals(batch1.length(), batch2.length());
                for (int j = 0; j < batch1.numData(); j++) {
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
