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
 *   Sep 8, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.LongSupplier;

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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.data.ArrowReadData;
import org.knime.core.columnar.arrow.data.ArrowWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.store.FileHandle;

/**
 * A static class with utility methods for arrow tests.
 *
 * @author
 */
public final class ArrowTestUtils {

    private ArrowTestUtils() {
        // Utility class
    }

    /**
     * Create a temporary file which is deleted on exit.
     *
     * @return the file
     * @throws IOException if the file could not be created
     */
    public static Path createTmpKNIMEArrowPath() throws IOException {
        final Path path = Files.createTempFile("KNIME-" + UUID.randomUUID().toString(), ".knarrow");
        path.toFile().deleteOnExit();
        return path;
    }

    /**
     * Create a FileSupplier that is backed by a temporary file which is deleted on exit.
     *
     * @return the fileSupplier
     * @throws IOException if the file backing the FileSupplier could not be created
     */
    public static FileHandle createTmpKNIMEArrowFileSupplier() throws IOException {
        return new TestFileSupplier(createTmpKNIMEArrowPath());
    }

    public static FileHandle createFileSupplier(final Path path) {
        return new TestFileSupplier(path);
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
                data.data[i] = random.nextInt();
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
        assertEquals(count, data.length());
        for (int i = 0; i < count; i++) {
            if (hasMissing && random.nextDouble() < 0.4) {
                assertTrue(data.isMissing(i));
            } else {
                assertFalse(data.isMissing(i));
                assertEquals(random.nextInt(), data.data[i]);
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
        assertEquals(numDistinct, data.getDictionarySize());
        for (int i = 0; i < numDistinct; i++) {
            assertEquals(random.nextLong(), data.getDictionaryValue(i));
        }

        // Check the data values
        for (int i = 0; i < count; i++) {
            assertFalse(data.isMissing(i));
            assertEquals(random.nextInt(numDistinct), data.getDataValue(i));
        }
    }

    /**
     * A simple implementation of {@link ArrowReadData} and {@link ArrowWriteData} for testing. Holds an {@code int[]}
     * array which can be accessed by {@link #data}. Use the {@link SimpleDataFactory} to create new instances.
     */
    public static final class SimpleData implements ArrowWriteData, ArrowReadData {

        private int[] data;

        private BitSet missing;

        private int length;

        public SimpleData(final int capacity) {
            data = new int[capacity];
            missing = new BitSet(capacity);
            length = 0;
        }

        @Override
        public int length() {
            return length;
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
            return data.length * Integer.BYTES + missing.size() / 8;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements * Integer.BYTES + missing.size() / 8;
        }

        @Override
        public int capacity() {
            return data.length;
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (data.length < minimumCapacity) {
                int newCapacity = data.length;
                while (newCapacity < minimumCapacity) {
                    newCapacity *= 2;
                }
                data = Arrays.copyOf(data, newCapacity);
                // BitSet expands automatically
            }
        }

        @Override
        public void setMissing(final int index) {
            missing.set(index);
        }

        @Override
        public SimpleData close(final int length) {
            if (length < 0 || length > capacity()) {
                throw new IllegalArgumentException("Invalid length");
            }
            this.length = length;
            return this;
        }

        @Override
        public boolean isMissing(final int index) {
            return missing.get(index);
        }

        //        @Override
        //        public ArrowReadData slice(final int start, final int length) {
        //            return null;
        //        }
        //
        //        @Override
        //        public ArrowWriteData slice(final int start) {
        //            return null;
        //        }
    }

    /**
     * A {@link ArrowReadData} and {@link ArrowWriteData} implementation holding a dictionary for testing. Holds an
     * index array which can be accessed with {@link #data} and a dictionary which can be accessed with
     * {@link #dictionaryValues}. Use {@link DictionaryEncodedDataFactory} to create new instances.
     */
    public static final class DictionaryEncodedData implements ArrowReadData, ArrowWriteData {

        private int[] data; // indices into the dictionary

        private BitSet missing;

        private long[] dictionaryValues;

        private int length;

        private final long dictionaryId;

        public DictionaryEncodedData(final int capacity, final int dictCapacity, final long dictionaryId) {
            data = new int[capacity];
            missing = new BitSet(capacity);
            dictionaryValues = new long[dictCapacity];
            length = 0;
            this.dictionaryId = dictionaryId;
        }

        @Override
        public int length() {
            return length;
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
            return data.length * Integer.BYTES + missing.size() / 8 + dictionaryValues.length * Long.BYTES;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements * Integer.BYTES + missing.size() / 8 + dictionaryValues.length * Long.BYTES;
        }

        @Override
        public int capacity() {
            return data.length;
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (data.length < minimumCapacity) {
                int newCapacity = data.length;
                while (newCapacity < minimumCapacity) {
                    newCapacity *= 2;
                }
                data = Arrays.copyOf(data, newCapacity);
            }
        }

        @Override
        public void setMissing(final int index) {
            missing.set(index);
        }

        @Override
        public DictionaryEncodedData close(final int length) {
            if (length < 0 || length > capacity()) {
                throw new IllegalArgumentException("Invalid length");
            }
            this.length = length;
            return this;
        }

        @Override
        public boolean isMissing(final int index) {
            return missing.get(index);
        }

        //        @Override
        //        public ArrowReadData slice(final int start, final int length) {
        //            return null;
        //        }
        //
        //        @Override
        //        public ArrowWriteData slice(final int start) {
        //            return null;
        //        }

        public void setDictionaryValue(final int index, final long value) {
            dictionaryValues[index] = value;
        }

        public long getDictionaryValue(final int index) {
            return dictionaryValues[index];
        }

        public void setDataValue(final int index, final int dictIndex) {
            data[index] = dictIndex;
        }

        public int getDataValue(final int index) {
            return data[index];
        }

        public int getDictionarySize() {
            return dictionaryValues.length;
        }

        public void setDictionarySize(final int size) {
            if (size > dictionaryValues.length) {
                dictionaryValues = Arrays.copyOf(dictionaryValues, size);
            }
        }
    }

    /** A factory for creating, reading and writing {@link SimpleData}. */
    public static final class SimpleDataFactory implements ArrowColumnDataFactory {

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
        public ArrowWriteData createWrite(final int capacity) {
            return new SimpleData(capacity);
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
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
                    data.data[i] = intVector.get(i);
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
                    intVector.set(i, simpleData.data[i]);
                }
            }
            intVector.setValueCount(length);
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            return null;
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

    /** A factory for creating, reading and writing {@link DictionaryEncodedData}. */
    public static final class DictionaryEncodedDataFactory implements ArrowColumnDataFactory {

        private long dictionaryId;

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            this.dictionaryId = dictionaryIdSupplier.getAsLong();
            final DictionaryEncoding dictionary = new DictionaryEncoding(dictionaryId, false, null);
            return new Field(name, new FieldType(true, MinorType.INT.getType(), dictionary), null);
        }

        @Override
        public ArrowWriteData createWrite(final int capacity) {
            int dictCapacity = 10; // default dictionary size
            return new DictionaryEncodedData(capacity, dictCapacity, dictionaryId);
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
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

            DictionaryEncodedData data = new DictionaryEncodedData(length, dictLength, dictionaryId);
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
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            DictionaryEncodedData dictData = (DictionaryEncodedData)data;
            // Create the dictionary vector
            BigIntVector dictVector = new BigIntVector("dictionary", null);
            int dictLength = dictData.getDictionarySize();
            dictVector.allocateNew(dictLength);
            for (int i = 0; i < dictLength; i++) {
                dictVector.set(i, dictData.getDictionaryValue(i));
            }
            dictVector.setValueCount(dictLength);

            // Create dictionary encoding
            DictionaryEncoding encoding = new DictionaryEncoding(dictData.dictionaryId, false, null);
            Dictionary dictionary = new Dictionary(dictVector, encoding);
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

    public static final class ComplexData implements ArrowReadData, ArrowWriteData {

        private int length;

        // Main vector fields
        private long[] fieldA;

        private int[] fieldBIndices; // indices into dictionaryB

        private List<Integer>[] fieldC;

        private int[] fieldD_eIndices; // indices into dictionaryE

        private boolean[] fieldD_f;

        private BitSet missing; // for the main vector

        // Dictionaries
        // Dictionary B
        private int[] dictB_gIndices; // indices into dictionaryG

        private int[] dictB_h;

        // Dictionary G
        private List<byte[]> dictG_values;

        // Dictionary E
        private List<byte[]> dictE_values;

        // Dictionary IDs
        public final long dictionaryIdB;

        public final long dictionaryIdE;

        public final long dictionaryIdG;

        @SuppressWarnings("unchecked")
        public ComplexData(final int capacity, final int dictBSize, final int dictGSize, final int dictESize,
            final long dictionaryIdB, final long dictionaryIdE, final long dictionaryIdG) {
            // initialize arrays with capacities
            fieldA = new long[capacity];
            fieldBIndices = new int[capacity];
            fieldC = new ArrayList[capacity];
            fieldD_eIndices = new int[capacity];
            fieldD_f = new boolean[capacity];
            missing = new BitSet(capacity);

            // Initialize dictionaries
            dictB_gIndices = new int[dictBSize];
            dictB_h = new int[dictBSize];

            dictG_values = new ArrayList<>(dictGSize);
            dictE_values = new ArrayList<>(dictESize);

            this.dictionaryIdB = dictionaryIdB;
            this.dictionaryIdE = dictionaryIdE;
            this.dictionaryIdG = dictionaryIdG;

            length = 0;
        }

        @Override
        public int length() {
            return length;
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
            size += fieldA.length * Long.BYTES;
            size += fieldBIndices.length * Integer.BYTES;
            size += fieldD_eIndices.length * Integer.BYTES;
            size += fieldD_f.length; // boolean[] is 1 byte per element?
            size += missing.size() / 8;

            for (List<Integer> list : fieldC) {
                if (list != null) {
                    size += list.size() * Integer.BYTES;
                }
            }

            size += dictB_gIndices.length * Integer.BYTES;
            size += dictB_h.length * Integer.BYTES;

            for (byte[] bytes : dictG_values) {
                size += bytes.length;
            }

            for (byte[] bytes : dictE_values) {
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
            return fieldA.length;
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (fieldA.length < minimumCapacity) {
                int newCapacity = fieldA.length;
                while (newCapacity < minimumCapacity) {
                    newCapacity *= 2;
                }

                fieldA = Arrays.copyOf(fieldA, newCapacity);
                fieldBIndices = Arrays.copyOf(fieldBIndices, newCapacity);
                fieldC = Arrays.copyOf(fieldC, newCapacity);
                fieldD_eIndices = Arrays.copyOf(fieldD_eIndices, newCapacity);
                fieldD_f = Arrays.copyOf(fieldD_f, newCapacity);
                missing = BitSet.valueOf(Arrays.copyOf(missing.toLongArray(), (newCapacity + 63) / 64));
            }
        }

        @Override
        public void setMissing(final int index) {
            missing.set(index);
        }

        @Override
        public boolean isMissing(final int index) {
            return missing.get(index);
        }

        @Override
        public ComplexData close(final int length) {
            if (length < 0 || length > capacity()) {
                throw new IllegalArgumentException("Invalid length");
            }
            this.length = length;
            return this;
        }

        //        @Override
        //        public ArrowReadData slice(final int start, final int length) {
        //            return null;
        //        }
        //
        //        @Override
        //        public ArrowWriteData slice(final int start) {
        //            return null;
        //        }

        // Getters and setters for data and dictionaries

        public void setFieldA(final int index, final long value) {
            fieldA[index] = value;
        }

        public long getFieldA(final int index) {
            return fieldA[index];
        }

        public void setFieldBIndex(final int index, final int dictIndex) {
            fieldBIndices[index] = dictIndex;
        }

        public int getFieldBIndex(final int index) {
            return fieldBIndices[index];
        }

        public void setFieldC(final int index, final List<Integer> list) {
            fieldC[index] = list;
        }

        public List<Integer> getFieldC(final int index) {
            return fieldC[index];
        }

        public void setFieldD_eIndex(final int index, final int dictIndex) {
            fieldD_eIndices[index] = dictIndex;
        }

        public int getFieldD_eIndex(final int index) {
            return fieldD_eIndices[index];
        }

        public void setFieldD_f(final int index, final boolean value) {
            fieldD_f[index] = value;
        }

        public boolean getFieldD_f(final int index) {
            return fieldD_f[index];
        }

        public void setDictB_gIndex(final int index, final int dictIndex) {
            dictB_gIndices[index] = dictIndex;
        }

        public int getDictB_gIndex(final int index) {
            return dictB_gIndices[index];
        }

        public void setDictB_h(final int index, final int value) {
            dictB_h[index] = value;
        }

        public int getDictB_h(final int index) {
            return dictB_h[index];
        }

        public void addDictGValue(final byte[] value) {
            dictG_values.add(value);
        }

        public byte[] getDictGValue(final int index) {
            return dictG_values.get(index);
        }

        public int getDictGSize() {
            return dictG_values.size();
        }

        public void addDictEValue(final byte[] value) {
            dictE_values.add(value);
        }

        public byte[] getDictEValue(final int index) {
            return dictE_values.get(index);
        }

        public int getDictESize() {
            return dictE_values.size();
        }

        public int getDictBSize() {
            return dictB_gIndices.length;
        }
    }

    public static final class ComplexDataFactory implements ArrowColumnDataFactory {

        private long dictionaryIdB;

        private long dictionaryIdE;

        private long dictionaryIdG;

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            // Get IDs for dictionaries
            dictionaryIdB = dictionaryIdSupplier.getAsLong();
            dictionaryIdE = dictionaryIdSupplier.getAsLong();
            dictionaryIdG = dictionaryIdSupplier.getAsLong();

            // Create DictionaryEncodings
            final DictionaryEncoding encodingB = new DictionaryEncoding(dictionaryIdB, false, null);
            final DictionaryEncoding encodingE = new DictionaryEncoding(dictionaryIdE, false, null);
            final DictionaryEncoding encodingG = new DictionaryEncoding(dictionaryIdG, false, null);

            // Most inner fields
            final Field a = new Field("a", FieldType.nullable(new ArrowType.Int(64, true)), null);
            final Field f = new Field("f", FieldType.nullable(new ArrowType.Bool()), null);
            final Field cChild = new Field("cChild", FieldType.nullable(new ArrowType.Int(32, true)), null);

            // Dictionary index vectors
            final Field b = new Field("b", new FieldType(true, new ArrowType.Int(32, true), encodingB), null);
            final Field e = new Field("e", new FieldType(true, new ArrowType.Int(32, true), encodingE), null);

            // Complex vectors
            final Field c = new Field("c", FieldType.nullable(ArrowType.List.INSTANCE), Arrays.asList(cChild));
            final Field d = new Field("d", FieldType.nullable(ArrowType.Struct.INSTANCE), Arrays.asList(e, f));

            // The final struct vector
            return new Field(name, FieldType.nullable(ArrowType.Struct.INSTANCE), Arrays.asList(a, b, c, d));
        }

        @Override
        public ArrowWriteData createWrite(final int capacity) {
            int dictBInitialSize = 10;
            int dictGInitialSize = 10;
            int dictEInitialSize = 10;
            return new ComplexData(capacity, dictBInitialSize, dictGInitialSize, dictEInitialSize, dictionaryIdB,
                dictionaryIdE, dictionaryIdG);
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
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

            ComplexData data =
                new ComplexData(length, dictBSize, dictGSize, dictESize, dictionaryIdB, dictionaryIdE, dictionaryIdG);

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

            BigIntVector vectorA = structVector.addOrGet("a",
                FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
            IntVector vectorB = structVector.addOrGet("b", new FieldType(true, new ArrowType.Int(32, true),
                new DictionaryEncoding(complexData.dictionaryIdB, false, null)), IntVector.class);
            ListVector vectorC = structVector.addOrGetList("c");
            StructVector vectorD = structVector.addOrGetStruct("d");
            IntVector vectorE = vectorD.addOrGet("e", new FieldType(true, new ArrowType.Int(32, true),
                new DictionaryEncoding(complexData.dictionaryIdE, false, null)), IntVector.class);
            BitVector vectorF =
                vectorD.addOrGet("f", FieldType.nullable(new ArrowType.Bool()), BitVector.class);

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

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            if (!(data instanceof ComplexData)) {
                return null;
            }
            ComplexData complexData = (ComplexData)data;

            VarBinaryVector dictVectorG = new VarBinaryVector("dictG", null);
            dictVectorG.allocateNew();
            List<byte[]> dictGValues = complexData.dictG_values;
            for (int i = 0; i < dictGValues.size(); i++) {
                dictVectorG.set(i, dictGValues.get(i));
            }
            dictVectorG.setValueCount(dictGValues.size());
            DictionaryEncoding encodingG = new DictionaryEncoding(complexData.dictionaryIdG, false, null);
            Dictionary dictionaryG = new Dictionary(dictVectorG, encodingG);

            StructVector dictVectorB = new StructVector("dictB", null, null);
            IntVector dictB_gIndices = dictVectorB.addOrGet("g", new FieldType(true, new ArrowType.Int(32, true),
                new DictionaryEncoding(complexData.dictionaryIdG, false, null)), IntVector.class);
            IntVector dictB_hValues =
                dictVectorB.addOrGet("h", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);

            dictB_gIndices.allocateNew(complexData.getDictBSize());
            dictB_hValues.allocateNew(complexData.getDictBSize());
            for (int i = 0; i < complexData.getDictBSize(); i++) {
                dictB_gIndices.set(i, complexData.getDictB_gIndex(i));
                dictB_hValues.set(i, complexData.getDictB_h(i));
            }
            dictB_gIndices.setValueCount(complexData.getDictBSize());
            dictB_hValues.setValueCount(complexData.getDictBSize());
            dictVectorB.setValueCount(complexData.getDictBSize());
            DictionaryEncoding encodingB = new DictionaryEncoding(complexData.dictionaryIdB, false, null);
            Dictionary dictionaryB = new Dictionary(dictVectorB, encodingB);

            VarBinaryVector dictVectorE = new VarBinaryVector("dictE", null);
            dictVectorE.allocateNew();
            List<byte[]> dictEValues = complexData.dictE_values;
            for (int i = 0; i < dictEValues.size(); i++) {
                dictVectorE.set(i, dictEValues.get(i));
            }
            dictVectorE.setValueCount(dictEValues.size());
            DictionaryEncoding encodingE = new DictionaryEncoding(complexData.dictionaryIdE, false, null);
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