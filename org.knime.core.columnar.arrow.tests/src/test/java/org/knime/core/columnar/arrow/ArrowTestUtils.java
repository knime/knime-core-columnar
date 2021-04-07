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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
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
import org.knime.core.columnar.ColumnarSchema;
import org.knime.core.columnar.arrow.data.ArrowReadData;
import org.knime.core.columnar.arrow.data.ArrowWriteData;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * A static class with utility methods for arrow tests.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
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
     * Create a schema with the given types for test purposes.
     *
     * @param types the types of the columns
     * @return the schema
     */
    public static ColumnarSchema createSchema(final DataSpec... types) {
        return new ColumnarSchema() {

            @Override
            public int numColumns() {
                return types.length;
            }

            @Override
            public DataSpec getSpec(final int index) {
                return types[index];
            }
        };
    }

    /**
     * Create a schema with the given type multiple times.
     *
     * @param type the type of the columns
     * @param width the number of columns
     * @return the schema
     */
    public static ColumnarSchema createWideSchema(final DataSpec type, final int width) {
        final DataSpec[] types = new DataSpec[width];
        for (int i = 0; i < width; i++) {
            types[i] = type;
        }
        return createSchema(types);
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
    @SuppressWarnings("resource")
    public static SimpleData fillData(final SimpleData data, final boolean hasMissing, final int count,
        final long seed) {
        final Random random = new Random(seed);
        for (int i = 0; i < count; i++) {
            if (hasMissing && random.nextDouble() < 0.4) {
                data.setMissing(i);
            } else {
                data.getVector().set(i, random.nextInt());
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
    @SuppressWarnings("resource")
    public static void checkData(final SimpleData data, final boolean hasMissing, final int count, final long seed) {
        final Random random = new Random(seed);
        assertEquals(count, data.getVector().getValueCount());
        for (int i = 0; i < count; i++) {
            if (hasMissing && random.nextDouble() < 0.4) {
                assertTrue(data.isMissing(i));
            } else {
                assertFalse(data.isMissing(i));
                assertEquals(random.nextInt(), data.getVector().get(i));
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
    @SuppressWarnings("resource")
    public static DictionaryEncodedData fillData(final DictionaryEncodedData data, final int count, final long seed) {
        final Random random = new Random(seed);
        final BigIntVector dictVector = (BigIntVector)data.getDictionary().getVector();
        final IntVector vector = data.getVector();

        // Set the dictionary
        final int numDistinct = random.nextInt(10) + 5;
        dictVector.allocateNew(numDistinct);
        for (int i = 0; i < numDistinct; i++) {
            dictVector.set(i, random.nextLong());
        }
        dictVector.setValueCount(numDistinct);

        // Set the values to indices
        for (int i = 0; i < count; i++) {
            vector.set(i, random.nextInt(numDistinct));
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
    @SuppressWarnings("resource")
    public static void checkData(final DictionaryEncodedData data, final int count, final long seed) {
        final Random random = new Random(seed);

        // The vector
        final IntVector vector = data.getVector();
        assertNotNull(vector.getField().getDictionary());

        // The dictionary
        final Dictionary dictionary = data.getDictionary();
        assertNotNull(dictionary);
        final DictionaryEncoding encoding = dictionary.getEncoding();
        assertNotNull(encoding);
        assertEquals(vector.getField().getDictionary().getId(), encoding.getId());
        assertFalse(encoding.isOrdered());
        assertEquals(MinorType.INT.getType(), encoding.getIndexType());
        final FieldVector dictVectorNotCasted = dictionary.getVector();
        assertNotNull(dictVectorNotCasted);
        assertTrue(dictVectorNotCasted instanceof BigIntVector);
        final BigIntVector dictVector = (BigIntVector)dictVectorNotCasted;

        // Check the dictionary
        final int numDistinct = random.nextInt(10) + 5;
        assertEquals(numDistinct, dictVector.getValueCount());
        for (int i = 0; i < numDistinct; i++) {
            assertFalse(dictVector.isNull(i));
            assertEquals(random.nextLong(), dictVector.get(i));
        }

        // Set the values to indices
        for (int i = 0; i < count; i++) {
            assertFalse(vector.isNull(i));
            assertEquals(random.nextInt(numDistinct), vector.get(i));
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
    @SuppressWarnings("resource")
    public static ComplexData fillData(final ComplexData data, final int count, final long seed) {
        final Random random = new Random(seed);

        // Fill dictionary G:
        final int numDistinctG = random.nextInt(5) + 5;
        final VarBinaryVector dictG = data.getDictionaryVectorG();
        dictG.allocateNew(numDistinctG);
        for (int i = 0; i < numDistinctG; i++) {
            dictG.set(i, nextBytes(random));
        }
        dictG.setValueCount(numDistinctG);

        // Fill dictionary E:
        final int numDistinctE = random.nextInt(5) + 5;
        final VarBinaryVector dictE = data.getDictionaryVectorE();
        dictE.allocateNew(numDistinctE);
        for (int i = 0; i < numDistinctE; i++) {
            dictE.set(i, nextBytes(random));
        }
        dictE.setValueCount(numDistinctE);

        // Fill dictionary B:
        final int numDistinctB = random.nextInt(5) + 5;
        final StructVector dictB = data.getDictionaryVectorB();
        final IntVector vectorG = data.getVectorG();
        final IntVector vectorH = data.getVectorH();
        vectorG.allocateNew(numDistinctB);
        vectorH.allocateNew(numDistinctB);
        for (int i = 0; i < numDistinctB; i++) {
            dictB.setIndexDefined(i);
            vectorG.set(i, random.nextInt(numDistinctG));
            vectorH.set(i, random.nextInt());
        }
        vectorG.setValueCount(numDistinctB);
        vectorH.setValueCount(numDistinctB);
        dictB.setValueCount(numDistinctB);

        // Fill the struct vector
        final StructVector vector = data.getVector();
        final BigIntVector vectorA = data.getVectorA();
        final IntVector vectorB = data.getVectorB();
        final ListVector vectorC = data.getVectorC();
        final UnionListWriter writerC = vectorC.getWriter();
        final StructVector vectorD = data.getVectorD();
        final IntVector vectorE = data.getVectorE();
        final BitVector vectorF = data.getVectorF();
        vectorA.allocateNew(count);
        vectorB.allocateNew(count);
        vectorE.allocateNew(count);
        vectorF.allocateNew(count);
        writerC.allocate();
        for (int i = 0; i < count; i++) {
            vectorA.set(i, random.nextLong());
            vectorB.set(i, random.nextInt(numDistinctB));
            writerC.setPosition(i);
            writerC.startList();
            for (int j = 0; j < random.nextInt(4); j++) {
                writerC.integer().writeInt(random.nextInt());
            }
            writerC.endList();
            vectorE.set(i, random.nextInt(numDistinctE));
            vectorF.set(i, random.nextBoolean() ? 1 : 0);
            vectorD.setIndexDefined(i);
            vector.setIndexDefined(i);
        }
        writerC.setValueCount(count);
        return data.close(count);
    }

    /**
     * Check the values in the {@link ComplexData}.
     *
     * @param data the data object
     * @param count the number of values to check
     * @param seed the seed defining the values
     */
    @SuppressWarnings("resource")
    public static void checkData(final ComplexData data, final int count, final long seed) {
        final Random random = new Random(seed);

        // Check dictionary G:
        final int numDistinctG = random.nextInt(5) + 5;
        final VarBinaryVector dictG = data.getDictionaryVectorG();
        assertNotNull(dictG);
        assertEquals(numDistinctG, dictG.getValueCount());
        for (int i = 0; i < numDistinctG; i++) {
            assertFalse(dictG.isNull(i));
            assertArrayEquals(nextBytes(random), dictG.get(i));
        }

        // Check dictionary E:
        final int numDistinctE = random.nextInt(5) + 5;
        final VarBinaryVector dictE = data.getDictionaryVectorE();
        assertNotNull(dictE);
        assertEquals(numDistinctE, dictE.getValueCount());
        for (int i = 0; i < numDistinctE; i++) {
            assertFalse(dictE.isNull(i));
            assertArrayEquals(nextBytes(random), dictE.get(i));
        }

        // Check dictionary B:
        final int numDistinctB = random.nextInt(5) + 5;
        final StructVector dictB = data.getDictionaryVectorB();
        assertNotNull(dictB);
        assertEquals(numDistinctB, dictB.getValueCount());
        final IntVector vectorG = data.getVectorG();
        assertNotNull(vectorG);
        assertEquals(numDistinctB, vectorG.getValueCount());
        final IntVector vectorH = data.getVectorH();
        assertNotNull(vectorH);
        assertEquals(numDistinctB, vectorH.getValueCount());
        for (int i = 0; i < numDistinctB; i++) {
            assertFalse(dictB.isNull(i));
            assertFalse(vectorG.isNull(i));
            assertFalse(vectorH.isNull(i));
            assertEquals(random.nextInt(numDistinctG), vectorG.get(i));
            assertEquals(random.nextInt(), vectorH.get(i));
        }

        // Fill the struct vector
        final StructVector vector = data.getVector();
        assertNotNull(vector);
        assertEquals(count, vector.getValueCount());
        final BigIntVector vectorA = data.getVectorA();
        assertNotNull(vectorA);
        assertEquals(count, vectorA.getValueCount());
        final IntVector vectorB = data.getVectorB();
        assertNotNull(vectorB);
        assertEquals(count, vectorB.getValueCount());
        final ListVector vectorC = data.getVectorC();
        assertNotNull(vectorC);
        assertEquals(count, vectorC.getValueCount());
        final StructVector vectorD = data.getVectorD();
        assertNotNull(vectorD);
        assertEquals(count, vectorD.getValueCount());
        final IntVector vectorE = data.getVectorE();
        assertNotNull(vectorE);
        assertEquals(count, vectorE.getValueCount());
        final BitVector vectorF = data.getVectorF();
        assertNotNull(vectorF);
        assertEquals(count, vectorF.getValueCount());
        for (int i = 0; i < count; i++) {
            assertFalse(vector.isNull(i));
            assertFalse(vectorA.isNull(i));
            assertEquals(random.nextLong(), vectorA.get(i));
            assertFalse(vectorB.isNull(i));
            assertEquals(random.nextInt(numDistinctB), vectorB.get(i));
            final Object valuesC = vectorC.getObject(i);
            assertTrue(valuesC instanceof List);
            @SuppressWarnings("unchecked")
            final List<Integer> valuesCList = (List<Integer>)valuesC;
            for (int j = 0; j < random.nextInt(4); j++) {
                assertEquals(random.nextInt(), (int)valuesCList.get(j));
            }
            assertFalse(vectorD.isNull(i));
            assertFalse(vectorE.isNull(i));
            assertEquals(random.nextInt(numDistinctE), vectorE.get(i));
            assertFalse(vectorF.isNull(i));
            assertEquals(random.nextBoolean(), vectorF.get(i) == 1);
        }
    }

    /** Utility to get a random amount of bytes from a random instance */
    private static final byte[] nextBytes(final Random random) {
        final byte[] bytes = new byte[random.nextInt(10) + 5];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * A simple implementation of {@link NullableReadData} and {@link NullableWriteData} for testing. Holds an
     * {@link IntVector} which can be accessed by {@link #getVector()}. Use the {@link SimpleDataFactory} to create new
     * instances.
     */
    public static final class SimpleData implements ArrowWriteData, ArrowReadData {

        private final IntVector m_vector;

        private SimpleData(final IntVector vector) {
            m_vector = vector;
        }

        /**
         * @return the vector holding the data
         */
        public IntVector getVector() {
            return m_vector;
        }

        @Override
        public int length() {
            return m_vector.getValueCount();
        }

        @Override
        public void release() {
            m_vector.close();
        }

        @Override
        public void retain() {
            throw new IllegalStateException("SimpleData can only be referenced by one instance.");
        }

        @Override
        public long sizeOf() {
            // Does not matter for this test data
            return 100;
        }

        @Override
        public int capacity() {
            return m_vector.getValueCapacity();
        }

        @Override
        public void expand(final int minimumCapacity) {
            while (m_vector.getValueCapacity() < minimumCapacity) {
                m_vector.reAlloc();
            }
        }

        @Override
        public void setMissing(final int index) {
            m_vector.setNull(index);
        }

        @Override
        public SimpleData close(final int length) {
            if (length < 0) {
                throw new IllegalArgumentException("Length must be non-negative.");
            }
            if (length > capacity()) {
                throw new IllegalArgumentException("Length must not be larger than capacity.");
            }
            m_vector.setValueCount(length);
            return this;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_vector.isNull(index);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            // Cannot be sliced. Not important for reading/writing
            return null;
        }

        @Override
        public ArrowWriteData slice(final int start) {
            // Cannot be sliced. Not important for reading/writing
            return null;
        }
    }

    /**
     * A {@link NullableReadData} and {@link NullableWriteData} implementation holding a dictionary for testing. Holds
     * an index vector which can be accessed with {@link #getVector()} and a dictionary which can be accessed with
     * {@link #getDictionary()}. Use {@link DictionaryEncodedDataFactory} to create new instances.
     */
    public static final class DictionaryEncodedData implements ArrowReadData, ArrowWriteData {

        private final IntVector m_vector;

        private final Dictionary m_dictionary;

        private DictionaryEncodedData(final IntVector vector, final Dictionary dictionary) {
            m_vector = vector;
            m_dictionary = dictionary;
        }

        /**
         * @return the vector holding the index data
         */
        public IntVector getVector() {
            return m_vector;
        }

        /**
         * @return the dictionary
         */
        public Dictionary getDictionary() {
            return m_dictionary;
        }

        @Override
        public int length() {
            return m_vector.getValueCount();
        }

        @Override
        public void release() {
            m_vector.close();
            m_dictionary.getVector().close();
        }

        @Override
        public void retain() {
            throw new IllegalStateException("SimpleData can only be referenced by one instance.");
        }

        @Override
        public long sizeOf() {
            // Does not matter for this test data
            return 100;
        }

        @Override
        public int capacity() {
            return m_vector.getValueCapacity();
        }

        @Override
        public void expand(final int minimumCapacity) {
            while (m_vector.getValueCapacity() < minimumCapacity) {
                m_vector.reAlloc();
            }
        }

        @Override
        public void setMissing(final int index) {
            m_vector.setNull(index);
        }

        @Override
        public DictionaryEncodedData close(final int length) {
            if (length < 0) {
                throw new IllegalArgumentException("Length must be non-negative.");
            }
            if (length > capacity()) {
                throw new IllegalArgumentException("Length must not be larger than capacity.");
            }
            m_vector.setValueCount(length);
            return this;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_vector.isNull(index);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            // Cannot be sliced. Not important for reading/writing
            return null;
        }

        @Override
        public ArrowWriteData slice(final int start) {
            // Cannot be sliced. Not important for reading/writing
            return null;
        }
    }

    /**
     * A complex implemenation of {@link NullableReadData} and {@link NullableWriteData}. Holds a struct vector with
     * multiple child vectors and three dictionary encodings of which 1 is recursive.
     * </p>
     * ArrowType: <code>Struct&lt;a:BigInt, b:Int, c:List&lt;Int&gt;, d:Struct&lt;e:Int, f:Bit&gt;&gt;</code> </br>
     * Dictionaries: <code>b:Struct&lt;g:Int, h:Int&gt;, g:VarBinary, e:VarBinary</code>
     */
    public static final class ComplexData implements ArrowReadData, ArrowWriteData {

        private final StructVector m_vector;

        private final Dictionary m_dictionaryB;

        private final Dictionary m_dictionaryE;

        private final Dictionary m_dictionaryG;

        ComplexData(final StructVector vector, final Dictionary dictionaryB, final Dictionary dictionaryE,
            final Dictionary dictionaryG) {
            m_vector = vector;
            m_dictionaryB = dictionaryB;
            m_dictionaryE = dictionaryE;
            m_dictionaryG = dictionaryG;
        }

        @SuppressWarnings("resource")
        ComplexData(final StructVector vector, final DictionaryProvider dictionaries) {
            m_vector = vector;
            final IntVector vectorB = vector.addOrGet("b", null, IntVector.class);
            m_dictionaryB = dictionaries.lookup(vectorB.getField().getDictionary().getId());
            final IntVector vectorE = getVectorD().addOrGet("e", null, IntVector.class);
            m_dictionaryE = dictionaries.lookup(vectorE.getField().getDictionary().getId());
            final StructVector dictionaryVectorB = getDictionaryVectorB();
            final IntVector vectorG = dictionaryVectorB.addOrGet("g", null, IntVector.class);
            m_dictionaryG = dictionaries.lookup(vectorG.getField().getDictionary().getId());
        }

        /** @return the vector */
        public StructVector getVector() {
            return m_vector;
        }

        /** @return vector A. See class javadoc. */
        public BigIntVector getVectorA() {
            return m_vector.addOrGet("a", new FieldType(true, MinorType.BIGINT.getType(), null), BigIntVector.class);
        }

        /** @return vector B. See class javadoc. */
        public IntVector getVectorB() {
            return m_vector.addOrGet("b", new FieldType(true, MinorType.INT.getType(), m_dictionaryB.getEncoding()),
                IntVector.class);
        }

        /** @return vector C. See class javadoc. */
        public ListVector getVectorC() {
            return m_vector.addOrGetList("c");
        }

        /** @return child of vector C. See class javadoc. */
        @SuppressWarnings("resource")
        public IntVector getVectorCChild() {
            return (IntVector)getVectorC().addOrGetVector(new FieldType(true, MinorType.INT.getType(), null))
                .getVector();
        }

        /** @return vector D. See class javadoc. */
        public StructVector getVectorD() {
            return m_vector.addOrGetStruct("d");
        }

        /** @return vector E. See class javadoc. */
        @SuppressWarnings("resource")
        public IntVector getVectorE() {
            return getVectorD().addOrGet("e", new FieldType(true, MinorType.INT.getType(), m_dictionaryE.getEncoding()),
                IntVector.class);
        }

        /** @return vector F. See class javadoc. */
        @SuppressWarnings("resource")
        public BitVector getVectorF() {
            return getVectorD().addOrGet("f", new FieldType(true, MinorType.BIT.getType(), null), BitVector.class);
        }

        /** @return vector G. See class javadoc. */
        @SuppressWarnings("resource")
        public IntVector getVectorG() {
            return getDictionaryVectorB().addOrGet("g",
                new FieldType(true, MinorType.INT.getType(), m_dictionaryG.getEncoding()), IntVector.class);
        }

        /** @return vector H. See class javadoc. */
        @SuppressWarnings("resource")
        public IntVector getVectorH() {
            return getDictionaryVectorB().addOrGet("h", new FieldType(true, MinorType.INT.getType(), null),
                IntVector.class);
        }

        /** @return vector for dictionary B. See class javadoc. */
        public StructVector getDictionaryVectorB() {
            return (StructVector)m_dictionaryB.getVector();
        }

        /** @return vector for dictionary G. See class javadoc. */
        public VarBinaryVector getDictionaryVectorG() {
            return (VarBinaryVector)m_dictionaryG.getVector();
        }

        /** @return vector for dictionary E. See class javadoc. */
        public VarBinaryVector getDictionaryVectorE() {
            return (VarBinaryVector)m_dictionaryE.getVector();
        }

        @Override
        public int length() {
            return m_vector.getValueCount();
        }

        @Override
        public void release() {
            m_vector.close();
            m_dictionaryG.getVector().close();
            m_dictionaryB.getVector().close();
            m_dictionaryE.getVector().close();
        }

        @Override
        public void retain() {
            throw new IllegalStateException("SimpleData can only be referenced by one instance.");
        }

        @Override
        public long sizeOf() {
            // Does not matter for this test data
            return 100;
        }

        @Override
        public int capacity() {
            return m_vector.getValueCapacity();
        }

        @Override
        public void expand(final int minimumCapacity) {
            while (m_vector.getValueCapacity() < minimumCapacity) {
                m_vector.reAlloc();
            }
        }

        @Override
        public void setMissing(final int index) {
            m_vector.setNull(index);
        }

        @Override
        public boolean isMissing(final int index) {
            return m_vector.isNull(index);
        }

        @Override
        public ComplexData close(final int length) {
            if (length < 0) {
                throw new IllegalArgumentException("Length must be non-negative.");
            }
            if (length > capacity()) {
                throw new IllegalArgumentException("Length must not be larger than capacity.");
            }
            m_vector.setValueCount(length);
            return this;
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            // Cannot be sliced. Not important for reading/writing
            return null;
        }

        @Override
        public ArrowWriteData slice(final int start) {
            // Cannot be sliced. Not important for reading/writing
            return null;
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
        public SimpleData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final IntVector v = (IntVector)vector;
            v.allocateNew(capacity);
            return new SimpleData(v);
        }

        @Override
        public SimpleData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) {
            assertEquals(m_version, version);
            assertTrue(vector instanceof IntVector);
            assertEquals(vector.getNullCount(), nullCount.getNullCount());
            assertThrows(ArrayIndexOutOfBoundsException.class, () -> nullCount.getChild(0));
            return new SimpleData((IntVector)vector);
        }

        @Override
        public FieldVector getVector(final NullableReadData data) {
            return ((SimpleData)data).m_vector;
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            return null;
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return m_version;
        }
    }

    /** A factory for creating, reading and writing {@link DictionaryEncodedData}. */
    public static final class DictionaryEncodedDataFactory implements ArrowColumnDataFactory {

        private static final DictionaryEncoding encoding(final long id) {
            return new DictionaryEncoding(id, false, null);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final DictionaryEncoding dictionary = encoding(dictionaryIdSupplier.getAsLong());
            return new Field(name, new FieldType(true, MinorType.INT.getType(), dictionary), null);
        }

        @Override
        @SuppressWarnings("resource")
        public DictionaryEncodedData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final IntVector v = (IntVector)vector;
            final BigIntVector dictionaryVector = new BigIntVector("BigInt", allocator);
            final Dictionary dictionary = new Dictionary(dictionaryVector, encoding(dictionaryIdSupplier.getAsLong()));
            v.allocateNew(capacity);
            return new DictionaryEncodedData(v, dictionary);
        }

        @Override
        public DictionaryEncodedData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) {
            assertTrue(vector instanceof IntVector);
            final Dictionary dictionary = provider.lookup(vector.getField().getDictionary().getId());
            assertNotNull(dictionary);
            assertEquals(vector.getNullCount(), nullCount.getNullCount());
            assertThrows(ArrayIndexOutOfBoundsException.class, () -> nullCount.getChild(0));
            return new DictionaryEncodedData((IntVector)vector, dictionary);
        }

        @Override
        public FieldVector getVector(final NullableReadData data) {
            return ((DictionaryEncodedData)data).m_vector;
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            final Dictionary dictionary = ((DictionaryEncodedData)data).m_dictionary;
            return new ArrowReaderWriterUtils.SingletonDictionaryProvider(dictionary);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return ArrowColumnDataFactoryVersion.version(0);
        }
    }

    /** A factory for creating, reading and writing {@link ComplexData} */
    public static final class ComplexDataFactory implements ArrowColumnDataFactory {

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
        @SuppressWarnings("resource")
        public ComplexData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final StructVector v = (StructVector)vector;

            // Encodings
            final DictionaryEncoding encodingB = encoding(dictionaryIdSupplier);
            final DictionaryEncoding encodingE = encoding(dictionaryIdSupplier);
            final DictionaryEncoding encodingG = encoding(dictionaryIdSupplier);

            // Vectors
            final Field dictVectorG = field("DictG", MinorType.VARBINARY);
            final Field dictVectorE = field("DictE", MinorType.VARBINARY);

            final Field h = field("h", MinorType.INT);
            final Field g = field("g", encodingG);
            final Field dictVectorB = field("DictB", MinorType.STRUCT, g, h);

            // Dictionaries
            final Dictionary dictionaryB = new Dictionary(dictVectorB.createVector(allocator), encodingB);
            final Dictionary dictionaryE = new Dictionary(dictVectorE.createVector(allocator), encodingE);
            final Dictionary dictionaryG = new Dictionary(dictVectorG.createVector(allocator), encodingG);

            return new ComplexData(v, dictionaryB, dictionaryE, dictionaryG);
        }

        @Override
        public ComplexData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) {
            assertTrue(vector instanceof StructVector);
            final StructVector v = (StructVector)vector;
            checkNullCounts(nullCount, v);
            return new ComplexData(v, provider);
        }

        @SuppressWarnings("resource")
        private static void checkNullCounts(final ArrowVectorNullCount nullCount, final StructVector v) {
            assertEquals(v.getNullCount(), nullCount.getNullCount());
            final FieldVector a = v.getChild("a");
            final FieldVector b = v.getChild("b");
            final ListVector c = (ListVector)v.getChild("c");
            final StructVector d = (StructVector)v.getChild("d");
            final ArrowVectorNullCount nullCountA = nullCount.getChild(0);
            final ArrowVectorNullCount nullCountB = nullCount.getChild(1);
            final ArrowVectorNullCount nullCountC = nullCount.getChild(2);
            final ArrowVectorNullCount nullCountD = nullCount.getChild(3);
            assertEquals(a.getNullCount(), nullCountA.getNullCount());
            assertEquals(b.getNullCount(), nullCountB.getNullCount());
            assertEquals(c.getNullCount(), nullCountC.getNullCount());
            assertEquals(c.getDataVector().getNullCount(), nullCountC.getChild(0).getNullCount());
            assertEquals(d.getNullCount(), nullCountD.getNullCount());
            assertEquals(d.getChild("e").getNullCount(), nullCountD.getChild(0).getNullCount());
            assertEquals(d.getChild("f").getNullCount(), nullCountD.getChild(1).getNullCount());
        }

        @Override
        public FieldVector getVector(final NullableReadData data) {
            return ((ComplexData)data).m_vector;
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            final ComplexData d = ((ComplexData)data);
            return new MapDictionaryProvider(d.m_dictionaryB, d.m_dictionaryE, d.m_dictionaryG);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return ArrowColumnDataFactoryVersion.version(0);
        }
    }

}
