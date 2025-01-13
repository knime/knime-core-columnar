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
 *   Jan 13, 2025 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowWriteData;
import org.knime.core.columnar.arrow.onheap.data.ValidityBuffer;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Arrow data implementations for tests.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class OnHeapTestData {

    private OnHeapTestData() {
    }

    /**
     * Set the values of a {@link SimpleData} object and close it.
     *
     * @param data the data object
     * @param hasMissing if missing values should be added
     * @param count the number of values to write
     * @param seed the seed defining the values
     * @return the closed {@link NullableReadData}.
     */
    public static SimpleData fillData(final SimpleData data, final boolean hasMissing, final int count,
        final long seed) {
        final Random random = new Random(seed);
        data.expand(count);
        for (int i = 0; i < count; i++) {
            if (hasMissing && random.nextDouble() < 0.4) {
                data.setMissing(i);
            } else {
                data.m_data[i] = random.nextInt();
            }
        }
        return data.close(count);
    }

    /**
     * Check the values in the {@link SimpleData}.
     *
     * @param data the data object
     * @param hasMissing if missing values were be added
     * @param count the number of values to check
     * @param seed the seed defining the values
     */
    public static void checkData(final SimpleData data, final boolean hasMissing, final int count, final long seed) {
        final Random random = new Random(seed);
        assertEquals(count, data.length(), "Data length should match the expected count.");
        for (int i = 0; i < count; i++) {
            if (hasMissing && random.nextDouble() < 0.4) {
                assertTrue(data.isMissing(i), "Value at index " + i + " should be missing.");
            } else {
                assertFalse(data.isMissing(i), "Value at index " + i + " should not be missing.");
                assertEquals(random.nextInt(), data.m_data[i], "Value at index " + i + " does not match.");
            }
        }
    }

    /**
     * Set the values of a {@link DictionaryEncodedData} object and close it.
     *
     * @param data the data object
     * @param count the number of values to write
     * @param seed the seed defining the values
     * @return the closed {@link NullableReadData}.
     */
    public static DictionaryEncodedData fillData(final DictionaryEncodedData data, final int count, final long seed) {
        final Random random = new Random(seed);
        // Set the dictionary
        final int numDistinct = random.nextInt(10) + 5;
        data.setDictionarySize(numDistinct);
        for (int i = 0; i < numDistinct; i++) {
            data.setDictionaryValue(i, random.nextLong());
        }

        // Set the values to indices
        for (int i = 0; i < count; i++) {
            data.setDataValue(i, random.nextInt(numDistinct));
        }
        return data.close(count);
    }

    /**
     * Check the values in the {@link DictionaryEncodedData}.
     *
     * @param data the data object
     * @param count the number of values to check
     * @param seed the seed defining the values
     */
    public static void checkData(final DictionaryEncodedData data, final int count, final long seed) {
        final Random random = new Random(seed);

        // The dictionary
        final int numDistinct = random.nextInt(10) + 5;
        assertEquals(numDistinct, data.getDictionarySize(),
            "Dictionary size should match the expected number of distinct values.");
        for (int i = 0; i < numDistinct; i++) {
            assertEquals(random.nextLong(), data.getDictionaryValue(i),
                "Dictionary value at index " + i + " does not match.");
        }

        // Check the data values
        for (int i = 0; i < count; i++) {
            assertFalse(data.isMissing(i), "Data value at index " + i + " should not be missing.");
            assertEquals(random.nextInt(numDistinct), data.getDataValue(i),
                "Data value at index " + i + " does not match.");
        }
    }

    /**
     * Set the values of a {@link ComplexData} object and close it.
     *
     * @param data the data object
     * @param count the number of values to write
     * @param seed the seed defining the values
     * @return the closed {@link NullableReadData}.
     */
    public static ComplexData fillData(final ComplexData data, final int count, final long seed) {
        final Random random = new Random(seed);

        // Fill dictionary G:
        final int numDistinctG = random.nextInt(5) + 5;
        // Clear and resize dictionary G if needed (it's initially sized at construction)
        // dictG_values is a List<byte[]>, we can just clear and add.
        data.m_dictGvalues.clear();
        for (int i = 0; i < numDistinctG; i++) {
            data.addDictGValue(nextBytes(random));
        }

        // Fill dictionary E:
        final int numDistinctE = random.nextInt(5) + 5;
        data.m_dictEvalues.clear();
        for (int i = 0; i < numDistinctE; i++) {
            data.addDictEValue(nextBytes(random));
        }

        // Fill dictionary B:
        final int numDistinctB = random.nextInt(5) + 5;
        // Resize dictB arrays if needed
        if (data.getDictBSize() != numDistinctB) {
            data.m_dictBgIndices = Arrays.copyOf(data.m_dictBgIndices, numDistinctB);
            data.m_dictBh = Arrays.copyOf(data.m_dictBh, numDistinctB);
        }
        for (int i = 0; i < numDistinctB; i++) {
            data.setDictB_gIndex(i, random.nextInt(numDistinctG));
            data.setDictB_h(i, random.nextInt());
        }

        // Fill the struct vector fields:
        // For each of the count values:
        // a: random long
        // b: random int in [0, numDistinctB)
        // c: a list of 0 to 3 integers
        // d:
        //   e: random int in [0, numDistinctE)
        //   f: random boolean
        // No missing values are introduced.
        data.expand(count);
        for (int i = 0; i < count; i++) {
            data.setFieldA(i, random.nextLong());
            data.setFieldBIndex(i, random.nextInt(numDistinctB));

            final int listSize = random.nextInt(4); // 0 to 3
            List<Integer> cList = new ArrayList<>(listSize);
            for (int j = 0; j < listSize; j++) {
                cList.add(random.nextInt());
            }
            data.setFieldC(i, cList);

            data.setFieldD_eIndex(i, random.nextInt(numDistinctE));
            data.setFieldD_f(i, random.nextBoolean());
        }

        return data.close(count);
    }

    /**
     * Check the values in the {@link ComplexData} to match what was set by {@link #fillData(ComplexData, int, long)}.
     *
     * @param data the data object
     * @param count the number of values to check
     * @param seed the seed defining the values
     */
    public static void checkData(final ComplexData data, final int count, final long seed) {
        final Random random = new Random(seed);

        assertEquals(count, data.length(), "Data length should match the expected count.");

        // Check dictionary G:
        final int numDistinctG = random.nextInt(5) + 5;
        assertEquals(numDistinctG, data.getDictGSize(),
            "Dictionary G size should match the expected number of distinct values.");
        for (int i = 0; i < numDistinctG; i++) {
            byte[] expected = nextBytes(random);
            byte[] actual = data.getDictGValue(i);
            assertTrue(Arrays.equals(expected, actual), "Dictionary G value at index " + i + " does not match.");
        }

        // Check dictionary E:
        final int numDistinctE = random.nextInt(5) + 5;
        assertEquals(numDistinctE, data.getDictESize(),
            "Dictionary E size should match the expected number of distinct values.");
        for (int i = 0; i < numDistinctE; i++) {
            byte[] expected = nextBytes(random);
            byte[] actual = data.getDictEValue(i);
            assertTrue(Arrays.equals(expected, actual), "Dictionary E value at index " + i + " does not match.");
        }

        // Check dictionary B:
        final int numDistinctB = random.nextInt(5) + 5;
        assertEquals(numDistinctB, data.getDictBSize(),
            "Dictionary B size should match the expected number of distinct values.");
        for (int i = 0; i < numDistinctB; i++) {
            int expectedGIndex = random.nextInt(numDistinctG);
            int expectedH = random.nextInt();
            int actualGIndex = data.getDictB_gIndex(i);
            int actualH = data.getDictB_h(i);

            assertEquals(expectedGIndex, actualGIndex, "Dictionary B 'g' value at index " + i + " does not match.");
            assertEquals(expectedH, actualH, "Dictionary B 'h' value at index " + i + " does not match.");
        }

        // Check the main vector fields:
        for (int i = 0; i < count; i++) {
            // None should be missing
            assertFalse(data.isMissing(i), "Value at index " + i + " should not be missing.");

            // a
            long expectedA = random.nextLong();
            long actualA = data.getFieldA(i);
            assertEquals(expectedA, actualA, "Field 'a' value at index " + i + " does not match.");

            // b
            int expectedBIndex = random.nextInt(numDistinctB);
            int actualBIndex = data.getFieldBIndex(i);
            assertEquals(expectedBIndex, actualBIndex,
                "Field 'b' (dictionary index) value at index " + i + " does not match.");

            // c
            int listSize = random.nextInt(4);
            List<Integer> expectedC = new ArrayList<>(listSize);
            for (int j = 0; j < listSize; j++) {
                expectedC.add(random.nextInt());
            }

            List<Integer> actualC = data.getFieldC(i);
            if (listSize == 0) {
                // The old code always wrote a list (even if empty), so null should not occur
                assertTrue(actualC != null && actualC.isEmpty(), "Field 'c' list at index " + i + " should be empty.");
            } else {
                assertTrue(actualC != null && actualC.size() == listSize,
                    "Field 'c' list at index " + i + " should have size " + listSize + ".");
                for (int j = 0; j < listSize; j++) {
                    assertEquals(expectedC.get(j), actualC.get(j),
                        "Field 'c' list element at index " + i + ", element " + j + " does not match.");
                }
            }

            // d
            // e
            int expectedEIndex = random.nextInt(numDistinctE);
            int actualEIndex = data.getFieldD_eIndex(i);
            assertEquals(expectedEIndex, actualEIndex,
                "Field 'd.e' (dictionary index) value at index " + i + " does not match.");

            // f
            boolean expectedF = random.nextBoolean();
            boolean actualF = data.getFieldD_f(i);
            assertEquals(expectedF, actualF, "Field 'd.f' (boolean) value at index " + i + " does not match.");
        }
    }

    /** Utility to get a random amount of bytes from a random instance, same as old implementation. */
    private static final byte[] nextBytes(final Random random) {
        final byte[] bytes = new byte[random.nextInt(10) + 5];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * A simple implementation of {@link OnHeapArrowReadData} and {@link OnHeapArrowWriteData} for testing. Holds an
     * {@code int[]} array which can be accessed by {@link #m_data}. Use the {@link SimpleDataFactory} to create new
     * instances.
     */
    public static final class SimpleData implements OnHeapArrowWriteData, OnHeapArrowReadData {

        private int[] m_data;

        private BitSet m_missing;

        private int m_length;

        public SimpleData(final int capacity) {
            m_data = new int[capacity];
            m_missing = new BitSet(capacity);
            m_length = 0;
        }

        @Override
        public int length() {
            return m_length;
        }

        @Override
        public void release() {
            // No-op
        }

        @Override
        public void retain() {
            throw new IllegalStateException("SimpleData can only be referenced by one instance.");
        }

        @Override
        public long sizeOf() {
            return m_data.length * Integer.BYTES + m_missing.size() / 8;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements * Integer.BYTES + m_missing.size() / 8;
        }

        @Override
        public int capacity() {
            return m_data.length;
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (m_data.length < minimumCapacity) {
                int newCapacity = m_data.length;
                while (newCapacity < minimumCapacity) {
                    newCapacity *= 2;
                }
                m_data = Arrays.copyOf(m_data, newCapacity);
                // BitSet expands automatically
            }
        }

        @Override
        public void setMissing(final int index) {
            m_missing.set(index);
        }

        @Override
        public SimpleData close(final int length) {
            if (length < 0 || length > capacity()) {
                throw new IllegalArgumentException("Invalid length");
            }
            this.m_length = length;
            return this;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_missing.get(index);
        }

        @Override
        public ValidityBuffer getValidityBuffer() {
            return null;
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return null;
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return null;
        }
    }

    /** A factory for creating, reading and writing {@link SimpleData}. */
    public static final class SimpleDataFactory implements OnHeapArrowColumnDataFactory {

        private final ArrowColumnDataFactoryVersion m_version;

        /** Create a factory for {@link SimpleData}. */
        public SimpleDataFactory() {
            this(ArrowColumnDataFactoryVersion.version(0));
        }

        /**
         * Create a factory for {@link SimpleData} with the given version. Checks the given version on
         * {@link #createRead(FieldVector, ArrowVectorNullCount, DictionaryProvider, ArrowColumnDataFactoryVersion)}.
         *
         * @param version the version
         */
        public SimpleDataFactory(final ArrowColumnDataFactoryVersion version) {
            m_version = version;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.INT.getType());
        }

        @Override
        public OnHeapArrowWriteData createWrite(final int capacity) {
            return new SimpleData(capacity);
        }

        @Override
        public OnHeapArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (!m_version.equals(version)) {
                throw new IOException("Version mismatch");
            }
            if (!(vector instanceof IntVector)) {
                throw new IOException("Expected IntVector");
            }
            IntVector intVector = (IntVector)vector;
            int length = intVector.getValueCount();
            SimpleData data = new SimpleData(length);
            for (int i = 0; i < length; i++) {
                if (intVector.isNull(i)) {
                    data.setMissing(i);
                } else {
                    data.m_data[i] = intVector.get(i);
                }
            }
            data.close(length);
            return data;
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            SimpleData simpleData = (SimpleData)data;
            if (!(vector instanceof IntVector)) {
                throw new IllegalArgumentException("Expected IntVector");
            }
            IntVector intVector = (IntVector)vector;
            int length = simpleData.length();
            intVector.allocateNew(length);
            for (int i = 0; i < length; i++) {
                if (simpleData.isMissing(i)) {
                    intVector.setNull(i);
                } else {
                    intVector.set(i, simpleData.m_data[i]);
                }
            }
            intVector.setValueCount(length);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return m_version;
        }

        @Override
        public int initialNumBytesPerElement() {
            return Integer.BYTES + 1;
        }
    }

    /**
     * A {@link OnHeapArrowReadData} and {@link OnHeapArrowWriteData} implementation holding a dictionary for testing.
     * Holds an index array which can be accessed with {@link #m_data} and a dictionary which can be accessed with
     * {@link #m_dictionaryValues}. Use {@link DictionaryEncodedDataFactory} to create new instances.
     */
    public static final class DictionaryEncodedData implements OnHeapArrowReadData, OnHeapArrowWriteData {

        private int[] m_data; // indices into the dictionary

        private BitSet m_missing;

        private long[] m_dictionaryValues;

        private int m_length;

        public DictionaryEncodedData(final int capacity, final int dictCapacity) {
            m_data = new int[capacity];
            m_missing = new BitSet(capacity);
            m_dictionaryValues = new long[dictCapacity];
            m_length = 0;
        }

        @Override
        public int length() {
            return m_length;
        }

        @Override
        public void release() {
            // No-op
        }

        @Override
        public void retain() {
            throw new IllegalStateException("DictionaryEncodedData can only be referenced by one instance.");
        }

        @Override
        public long sizeOf() {
            return m_data.length * Integer.BYTES + m_missing.size() / 8 + m_dictionaryValues.length * Long.BYTES;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements * Integer.BYTES + m_missing.size() / 8 + m_dictionaryValues.length * Long.BYTES;
        }

        @Override
        public int capacity() {
            return m_data.length;
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (m_data.length < minimumCapacity) {
                int newCapacity = m_data.length;
                while (newCapacity < minimumCapacity) {
                    newCapacity *= 2;
                }
                m_data = Arrays.copyOf(m_data, newCapacity);
            }
        }

        @Override
        public void setMissing(final int index) {
            m_missing.set(index);
        }

        @Override
        public DictionaryEncodedData close(final int length) {
            if (length < 0 || length > capacity()) {
                throw new IllegalArgumentException("Invalid length");
            }
            this.m_length = length;
            return this;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_missing.get(index);
        }

        public void setDictionaryValue(final int index, final long value) {
            m_dictionaryValues[index] = value;
        }

        public long getDictionaryValue(final int index) {
            return m_dictionaryValues[index];
        }

        public void setDataValue(final int index, final int dictIndex) {
            m_data[index] = dictIndex;
        }

        public int getDataValue(final int index) {
            return m_data[index];
        }

        public int getDictionarySize() {
            return m_dictionaryValues.length;
        }

        public void setDictionarySize(final int size) {
            m_dictionaryValues = Arrays.copyOf(m_dictionaryValues, size);
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return null;
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return null;
        }

        @Override
        public ValidityBuffer getValidityBuffer() {
            return null;
        }
    }

    /** A factory for creating, reading and writing {@link DictionaryEncodedData}. */
    public static final class DictionaryEncodedDataFactory implements OnHeapArrowColumnDataFactory {

        private static DictionaryEncoding encoding(final LongSupplier dictionaryIdSupplier) {
            return new DictionaryEncoding(dictionaryIdSupplier.getAsLong(), false, null);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final DictionaryEncoding dictionary = encoding(dictionaryIdSupplier);
            return new Field(name, new FieldType(true, MinorType.INT.getType(), dictionary), null);
        }

        @Override
        public OnHeapArrowWriteData createWrite(final int capacity) {
            int dictCapacity = 10; // default dictionary size
            return new DictionaryEncodedData(capacity, dictCapacity);
        }

        @Override
        @SuppressWarnings("resource")
        public OnHeapArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (!(vector instanceof IntVector)) {
                throw new IOException("Expected IntVector");
            }
            IntVector intVector = (IntVector)vector;
            DictionaryEncoding encoding = intVector.getField().getDictionary();
            if (encoding == null) {
                throw new IOException("Expected dictionary encoding");
            }
            long dictionaryId = encoding.getId();
            Dictionary dictionary = provider.lookup(dictionaryId);
            if (dictionary == null) {
                throw new IOException("Dictionary not found");
            }
            if (!(dictionary.getVector() instanceof BigIntVector)) {
                throw new IOException("Expected BigIntVector for dictionary values");
            }
            BigIntVector dictVector = (BigIntVector)dictionary.getVector();

            int length = intVector.getValueCount();
            int dictLength = dictVector.getValueCount();

            DictionaryEncodedData data = new DictionaryEncodedData(length, dictLength);
            // Read dictionary values
            for (int i = 0; i < dictLength; i++) {
                if (dictVector.isNull(i)) {
                    throw new IOException("Null values in dictionary are not supported");
                }
                data.setDictionaryValue(i, dictVector.get(i));
            }
            // Read data values
            for (int i = 0; i < length; i++) {
                if (intVector.isNull(i)) {
                    data.setMissing(i);
                } else {
                    data.setDataValue(i, intVector.get(i));
                }
            }
            data.close(length);
            return data;
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            DictionaryEncodedData dictData = (DictionaryEncodedData)data;
            if (!(vector instanceof IntVector)) {
                throw new IllegalArgumentException("Expected IntVector");
            }
            IntVector intVector = (IntVector)vector;
            int length = dictData.length();
            intVector.allocateNew(length);
            for (int i = 0; i < length; i++) {
                if (dictData.isMissing(i)) {
                    intVector.setNull(i);
                } else {
                    intVector.set(i, dictData.getDataValue(i));
                }
            }
            intVector.setValueCount(length);
        }

        @Override
        @Deprecated
        @SuppressWarnings("resource")
        public DictionaryProvider createDictionaries(final NullableReadData data,
            final LongSupplier dictionaryIdSupplier, final BufferAllocator allocator) {
            DictionaryEncodedData dictData = (DictionaryEncodedData)data;
            // Create the dictionary vector
            BigIntVector dictVector = new BigIntVector("dictionary", allocator);
            int dictLength = dictData.getDictionarySize();
            dictVector.allocateNew(dictLength);
            for (int i = 0; i < dictLength; i++) {
                dictVector.set(i, dictData.getDictionaryValue(i));
            }
            dictVector.setValueCount(dictLength);

            // Create dictionary encoding
            Dictionary dictionary = new Dictionary(dictVector, encoding(dictionaryIdSupplier));
            return new MapDictionaryProvider(dictionary);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return ArrowColumnDataFactoryVersion.version(0);
        }

        @Override
        public int initialNumBytesPerElement() {
            return Integer.BYTES + 1;
        }
    }

    public static final class ComplexData implements OnHeapArrowReadData, OnHeapArrowWriteData {

        private int m_length;

        // Main vector fields
        private long[] m_fieldA;

        private int[] m_fieldBIndices; // indices into dictionaryB

        private List<Integer>[] m_fieldC;

        private int[] m_fieldDeIndices; // indices into dictionaryE

        private boolean[] m_fieldDf;

        private BitSet m_missing; // for the main vector

        // Dictionaries
        // Dictionary B
        private int[] m_dictBgIndices; // indices into dictionaryG

        private int[] m_dictBh;

        // Dictionary G
        private List<byte[]> m_dictGvalues;

        // Dictionary E
        private List<byte[]> m_dictEvalues;

        @SuppressWarnings("unchecked")
        public ComplexData(final int capacity, final int dictBSize, final int dictGSize, final int dictESize) {
            // initialize arrays with capacities
            m_fieldA = new long[capacity];
            m_fieldBIndices = new int[capacity];
            m_fieldC = new ArrayList[capacity];
            m_fieldDeIndices = new int[capacity];
            m_fieldDf = new boolean[capacity];
            m_missing = new BitSet(capacity);

            // Initialize dictionaries
            m_dictBgIndices = new int[dictBSize];
            m_dictBh = new int[dictBSize];

            m_dictGvalues = new ArrayList<>(dictGSize);
            m_dictEvalues = new ArrayList<>(dictESize);

            m_length = 0;
        }

        @Override
        public int length() {
            return m_length;
        }

        @Override
        public void release() {
            // No-op
        }

        @Override
        public void retain() {
            throw new IllegalStateException("ComplexData can only be referenced by one instance.");
        }

        @Override
        public long sizeOf() {
            // Implement size calculation
            long size = 0;
            size += m_fieldA.length * Long.BYTES;
            size += m_fieldBIndices.length * Integer.BYTES;
            size += m_fieldDeIndices.length * Integer.BYTES;
            size += m_fieldDf.length; // boolean[] is 1 byte per element?
            size += m_missing.size() / 8;

            for (List<Integer> list : m_fieldC) {
                if (list != null) {
                    size += list.size() * Integer.BYTES;
                }
            }

            size += m_dictBgIndices.length * Integer.BYTES;
            size += m_dictBh.length * Integer.BYTES;

            for (byte[] bytes : m_dictGvalues) {
                size += bytes.length;
            }

            for (byte[] bytes : m_dictEvalues) {
                size += bytes.length;
            }

            return size;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            // Similar to sizeOf, but only up to numElements
            return sizeOf(); // Simplify for now
        }

        @Override
        public int capacity() {
            return m_fieldA.length;
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (m_fieldA.length < minimumCapacity) {
                int newCapacity = m_fieldA.length;
                while (newCapacity < minimumCapacity) {
                    newCapacity *= 2;
                }

                m_fieldA = Arrays.copyOf(m_fieldA, newCapacity);
                m_fieldBIndices = Arrays.copyOf(m_fieldBIndices, newCapacity);
                m_fieldC = Arrays.copyOf(m_fieldC, newCapacity);
                m_fieldDeIndices = Arrays.copyOf(m_fieldDeIndices, newCapacity);
                m_fieldDf = Arrays.copyOf(m_fieldDf, newCapacity);
                m_missing = BitSet.valueOf(Arrays.copyOf(m_missing.toLongArray(), (newCapacity + 63) / 64));
            }
        }

        @Override
        public void setMissing(final int index) {
            m_missing.set(index);
        }

        @Override
        public boolean isMissing(final int index) {
            return m_missing.get(index);
        }

        @Override
        public ComplexData close(final int length) {
            if (length < 0 || length > capacity()) {
                throw new IllegalArgumentException("Invalid length");
            }
            this.m_length = length;
            return this;
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return null;
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return null;
        }

        @Override
        public ValidityBuffer getValidityBuffer() {
            return null;
        }

        // Getters and setters for data and dictionaries

        public void setFieldA(final int index, final long value) {
            m_fieldA[index] = value;
        }

        public long getFieldA(final int index) {
            return m_fieldA[index];
        }

        public void setFieldBIndex(final int index, final int dictIndex) {
            m_fieldBIndices[index] = dictIndex;
        }

        public int getFieldBIndex(final int index) {
            return m_fieldBIndices[index];
        }

        public void setFieldC(final int index, final List<Integer> list) {
            m_fieldC[index] = list;
        }

        public List<Integer> getFieldC(final int index) {
            return m_fieldC[index];
        }

        public void setFieldD_eIndex(final int index, final int dictIndex) {
            m_fieldDeIndices[index] = dictIndex;
        }

        public int getFieldD_eIndex(final int index) {
            return m_fieldDeIndices[index];
        }

        public void setFieldD_f(final int index, final boolean value) {
            m_fieldDf[index] = value;
        }

        public boolean getFieldD_f(final int index) {
            return m_fieldDf[index];
        }

        public void setDictB_gIndex(final int index, final int dictIndex) {
            m_dictBgIndices[index] = dictIndex;
        }

        public int getDictB_gIndex(final int index) {
            return m_dictBgIndices[index];
        }

        public void setDictB_h(final int index, final int value) {
            m_dictBh[index] = value;
        }

        public int getDictB_h(final int index) {
            return m_dictBh[index];
        }

        public void addDictGValue(final byte[] value) {
            m_dictGvalues.add(value);
        }

        public byte[] getDictGValue(final int index) {
            return m_dictGvalues.get(index);
        }

        public int getDictGSize() {
            return m_dictGvalues.size();
        }

        public void addDictEValue(final byte[] value) {
            m_dictEvalues.add(value);
        }

        public byte[] getDictEValue(final int index) {
            return m_dictEvalues.get(index);
        }

        public int getDictESize() {
            return m_dictEvalues.size();
        }

        public int getDictBSize() {
            return m_dictBgIndices.length;
        }
    }

    public static final class ComplexDataFactory implements OnHeapArrowColumnDataFactory {

        private static DictionaryEncoding encoding(final LongSupplier dictionaryIdSupplier) {
            return new DictionaryEncoding(dictionaryIdSupplier.getAsLong(), false, null);
        }

        private static Field field(final String name, final MinorType type) {
            return Field.nullable(name, type.getType());
        }

        private static Field field(final String name, final DictionaryEncoding encoding) {
            return new Field(name, new FieldType(true, MinorType.INT.getType(), encoding), null);
        }

        private static Field field(final String name, final MinorType type, final Field... children) {
            return new Field(name, new FieldType(true, type.getType(), null), Arrays.asList(children));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final DictionaryEncoding encodingB = encoding(dictionaryIdSupplier);
            final DictionaryEncoding encodingE = encoding(dictionaryIdSupplier);
            // Id for dictionary G. We cannot create the field yet because it is a child of a dictionary
            dictionaryIdSupplier.getAsLong();

            // Most inner fields
            final Field a = field("a", MinorType.BIGINT);
            final Field f = field("f", MinorType.BIT);
            final Field cChild = field("cChild", MinorType.INT);

            // Dictionary index vectors
            final Field b = field("b", encodingB);
            final Field e = field("e", encodingE);

            // Complex vectors
            final Field c = field("c", MinorType.LIST, cChild);
            final Field d = field("d", MinorType.STRUCT, e, f);

            // The final struct vector
            return field(name, MinorType.STRUCT, a, b, c, d);
        }

        @Override
        public OnHeapArrowWriteData createWrite(final int capacity) {
            int dictBInitialSize = 10;
            int dictGInitialSize = 10;
            int dictEInitialSize = 10;
            return new ComplexData(capacity, dictBInitialSize, dictGInitialSize, dictEInitialSize);
        }

        @Override
        @SuppressWarnings("resource")
        public OnHeapArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (!getVersion().equals(version)) {
                throw new IOException("Version mismatch");
            }
            if (!(vector instanceof StructVector)) {
                throw new IOException("Expected StructVector");
            }

            StructVector structVector = (StructVector)vector;
            int length = structVector.getValueCount();

            FieldVector vectorA = structVector.getChild("a");
            FieldVector vectorB = structVector.getChild("b");
            FieldVector vectorC = structVector.getChild("c");
            FieldVector vectorD = structVector.getChild("d");

            if (!(vectorA instanceof BigIntVector)) {
                throw new IOException("Expected BigIntVector for field 'a'");
            }
            if (!(vectorB instanceof IntVector)) {
                throw new IOException("Expected IntVector for field 'b'");
            }
            if (!(vectorC instanceof ListVector)) {
                throw new IOException("Expected ListVector for field 'c'");
            }
            if (!(vectorD instanceof StructVector)) {
                throw new IOException("Expected StructVector for field 'd'");
            }

            // Get dictionaries
            DictionaryEncoding encodingB = vectorB.getField().getDictionary();
            if (encodingB == null) {
                throw new IOException("Missing dictionary encoding for field 'b'");
            }
            long dictionaryIdB = encodingB.getId();
            Dictionary dictionaryB = provider.lookup(dictionaryIdB);
            if (dictionaryB == null) {
                throw new IOException("Dictionary not found for ID " + dictionaryIdB);
            }
            StructVector dictStructVectorB = (StructVector)dictionaryB.getVector();

            FieldVector dictB_g = dictStructVectorB.getChild("g");
            FieldVector dictB_h = dictStructVectorB.getChild("h");

            if (!(dictB_g instanceof IntVector)) {
                throw new IOException("Expected IntVector for field 'g' in dictionary 'B'");
            }
            if (!(dictB_h instanceof IntVector)) {
                throw new IOException("Expected IntVector for field 'h' in dictionary 'B'");
            }

            DictionaryEncoding encodingG = dictB_g.getField().getDictionary();
            if (encodingG == null) {
                throw new IOException("Missing dictionary encoding for field 'g' in dictionary 'B'");
            }
            long dictionaryIdG = encodingG.getId();
            Dictionary dictionaryG = provider.lookup(dictionaryIdG);
            if (dictionaryG == null) {
                throw new IOException("Dictionary not found for ID " + dictionaryIdG);
            }
            VarBinaryVector dictVarBinaryVectorG = (VarBinaryVector)dictionaryG.getVector();

            StructVector structVectorD = (StructVector)vectorD;
            FieldVector vectorE = structVectorD.getChild("e");
            FieldVector vectorF = structVectorD.getChild("f");

            if (!(vectorE instanceof IntVector)) {
                throw new IOException("Expected IntVector for field 'e' in 'd'");
            }
            if (!(vectorF instanceof BitVector)) {
                throw new IOException("Expected BitVector for field 'f' in 'd'");
            }

            DictionaryEncoding encodingE = vectorE.getField().getDictionary();
            if (encodingE == null) {
                throw new IOException("Missing dictionary encoding for field 'e' in 'd'");
            }
            long dictionaryIdE = encodingE.getId();
            Dictionary dictionaryE = provider.lookup(dictionaryIdE);
            if (dictionaryE == null) {
                throw new IOException("Dictionary not found for ID " + dictionaryIdE);
            }
            VarBinaryVector dictVarBinaryVectorE = (VarBinaryVector)dictionaryE.getVector();

            int dictBSize = dictStructVectorB.getValueCount();
            int dictGSize = dictVarBinaryVectorG.getValueCount();
            int dictESize = dictVarBinaryVectorE.getValueCount();

            ComplexData data = new ComplexData(length, dictBSize, dictGSize, dictESize);

            // Read dictionary G
            for (int i = 0; i < dictGSize; i++) {
                if (dictVarBinaryVectorG.isNull(i)) {
                    throw new IOException("Null values in dictionary 'G' are not supported");
                }
                data.addDictGValue(dictVarBinaryVectorG.get(i));
            }

            // Read dictionary E
            for (int i = 0; i < dictESize; i++) {
                if (dictVarBinaryVectorE.isNull(i)) {
                    throw new IOException("Null values in dictionary 'E' are not supported");
                }
                data.addDictEValue(dictVarBinaryVectorE.get(i));
            }

            // Read dictionary B
            IntVector dictB_gIndices = (IntVector)dictB_g;
            IntVector dictB_hValues = (IntVector)dictB_h;
            for (int i = 0; i < dictBSize; i++) {
                if (dictB_gIndices.isNull(i)) {
                    throw new IOException("Null values in field 'g' of dictionary 'B' are not supported");
                }
                if (dictB_hValues.isNull(i)) {
                    throw new IOException("Null values in field 'h' of dictionary 'B' are not supported");
                }
                data.setDictB_gIndex(i, dictB_gIndices.get(i));
                data.setDictB_h(i, dictB_hValues.get(i));
            }

            // Read main data
            BigIntVector vectorAValues = (BigIntVector)vectorA;
            IntVector vectorBIndices = (IntVector)vectorB;
            ListVector vectorCList = (ListVector)vectorC;
            StructVector vectorDStruct = (StructVector)vectorD;
            IntVector vectorEIndices = (IntVector)vectorE;
            BitVector vectorFValues = (BitVector)vectorF;

            for (int i = 0; i < length; i++) {
                if (vectorAValues.isNull(i)) {
                    data.setMissing(i);
                } else {
                    data.setFieldA(i, vectorAValues.get(i));
                }
                if (vectorBIndices.isNull(i)) {
                    throw new IOException("Null values in field 'b' are not supported");
                }
                data.setFieldBIndex(i, vectorBIndices.get(i));

                if (vectorCList.isNull(i)) {
                    data.setFieldC(i, null);
                } else {
                    List<Integer> list = new ArrayList<>();
                    int start = vectorCList.getElementStartIndex(i);
                    int end = vectorCList.getElementEndIndex(i);
                    IntVector vectorCData = (IntVector)vectorCList.getDataVector();
                    for (int j = start; j < end; j++) {
                        if (vectorCData.isNull(j)) {
                            throw new IOException("Null values in list 'c' are not supported");
                        }
                        list.add(vectorCData.get(j));
                    }
                    data.setFieldC(i, list);
                }

                if (vectorDStruct.isNull(i)) {
                    throw new IOException("Null values in field 'd' are not supported");
                }

                if (vectorEIndices.isNull(i)) {
                    throw new IOException("Null values in field 'e' are not supported");
                }
                data.setFieldD_eIndex(i, vectorEIndices.get(i));

                if (vectorFValues.isNull(i)) {
                    throw new IOException("Null values in field 'f' are not supported");
                }
                data.setFieldD_f(i, vectorFValues.get(i) == 1);
            }

            data.close(length);

            return data;
        }

        @Override
        @SuppressWarnings("resource")
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            if (!(data instanceof ComplexData)) {
                throw new IllegalArgumentException("Expected ComplexData");
            }
            if (!(vector instanceof StructVector)) {
                throw new IllegalArgumentException("Expected StructVector");
            }
            ComplexData complexData = (ComplexData)data;
            StructVector structVector = (StructVector)vector;
            int length = complexData.length();

            BigIntVector vectorA = structVector.getChild("a", BigIntVector.class);
            IntVector vectorB = structVector.getChild("b", IntVector.class);
            ListVector vectorC = structVector.getChild("c", ListVector.class);
            StructVector vectorD = structVector.getChild("d", StructVector.class);
            IntVector vectorE = vectorD.getChild("e", IntVector.class);
            BitVector vectorF = vectorD.getChild("f", BitVector.class);

            vectorA.allocateNew(length);
            vectorB.allocateNew(length);
            vectorE.allocateNew(length);
            vectorF.allocateNew(length);

            UnionListWriter listWriter = vectorC.getWriter();

            for (int i = 0; i < length; i++) {
                if (complexData.isMissing(i)) {
                    vectorA.setNull(i);
                } else {
                    vectorA.set(i, complexData.getFieldA(i));
                }

                vectorB.set(i, complexData.getFieldBIndex(i));

                List<Integer> list = complexData.getFieldC(i);
                if (list == null) {
                    vectorC.setNull(i);
                } else {
                    listWriter.setPosition(i);
                    listWriter.startList();
                    for (Integer value : list) {
                        listWriter.integer().writeInt(value);
                    }
                    listWriter.endList();
                }

                vectorD.setIndexDefined(i);

                vectorE.set(i, complexData.getFieldD_eIndex(i));

                vectorF.set(i, complexData.getFieldD_f(i) ? 1 : 0);
            }

            vectorA.setValueCount(length);
            vectorB.setValueCount(length);
            vectorC.setValueCount(length);
            vectorE.setValueCount(length);
            vectorF.setValueCount(length);

            structVector.setValueCount(length);
        }

        @Deprecated
        @Override
        @SuppressWarnings("resource")
        public DictionaryProvider createDictionaries(final NullableReadData data,
            final LongSupplier dictionaryIdSupplier, final BufferAllocator allocator) {
            if (!(data instanceof ComplexData)) {
                return null;
            }
            ComplexData complexData = (ComplexData)data;

            // Encodings
            final DictionaryEncoding encodingB = encoding(dictionaryIdSupplier);
            final DictionaryEncoding encodingE = encoding(dictionaryIdSupplier);
            final DictionaryEncoding encodingG = encoding(dictionaryIdSupplier);

            // Vectors

            // Dictionary G
            var dictVectorG = (VarBinaryVector)field("DictG", MinorType.VARBINARY).createVector(allocator);
            dictVectorG.allocateNew();
            List<byte[]> dictGValues = complexData.m_dictGvalues;
            for (int i = 0; i < dictGValues.size(); i++) {
                dictVectorG.set(i, dictGValues.get(i));
            }
            dictVectorG.setValueCount(dictGValues.size());
            Dictionary dictionaryG = new Dictionary(dictVectorG, encodingG);

            // Dictionary B
            var dictVectorB =
                (StructVector)field("DictB", MinorType.STRUCT, field("g", encodingG), field("h", MinorType.INT))
                    .createVector(allocator);
            IntVector dictB_gIndices = dictVectorB.getChild("g", IntVector.class);
            IntVector dictB_hValues = dictVectorB.getChild("h", IntVector.class);

            dictB_gIndices.allocateNew(complexData.getDictBSize());
            dictB_hValues.allocateNew(complexData.getDictBSize());
            for (int i = 0; i < complexData.getDictBSize(); i++) {
                dictB_gIndices.set(i, complexData.getDictB_gIndex(i));
                dictB_hValues.set(i, complexData.getDictB_h(i));
            }
            dictB_gIndices.setValueCount(complexData.getDictBSize());
            dictB_hValues.setValueCount(complexData.getDictBSize());
            dictVectorB.setValueCount(complexData.getDictBSize());
            Dictionary dictionaryB = new Dictionary(dictVectorB, encodingB);

            // Dictionary E
            var dictVectorE = (VarBinaryVector)field("DictE", MinorType.VARBINARY).createVector(allocator);
            dictVectorE.allocateNew();
            List<byte[]> dictEValues = complexData.m_dictEvalues;
            for (int i = 0; i < dictEValues.size(); i++) {
                dictVectorE.set(i, dictEValues.get(i));
            }
            dictVectorE.setValueCount(dictEValues.size());
            Dictionary dictionaryE = new Dictionary(dictVectorE, encodingE);

            return new MapDictionaryProvider(dictionaryB, dictionaryE, dictionaryG);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return ArrowColumnDataFactoryVersion.version(0);
        }

        @Override
        public int initialNumBytesPerElement() {
            return 8 + 4 + 4 + 1 + 1;
        }
    }
}
