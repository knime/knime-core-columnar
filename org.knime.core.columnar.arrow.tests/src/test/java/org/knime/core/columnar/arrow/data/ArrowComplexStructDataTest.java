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
 *   Oct 14, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedObjectData.ArrowDictEncodedObjectDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedObjectData.ArrowDictEncodedObjectReadData;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedObjectData.ArrowDictEncodedObjectWriteData;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntReadData;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntWriteData;
import org.knime.core.columnar.arrow.data.ArrowObjectData.ArrowObjectDataFactory;
import org.knime.core.columnar.arrow.data.ArrowObjectData.ArrowObjectReadData;
import org.knime.core.columnar.arrow.data.ArrowObjectData.ArrowObjectWriteData;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;

/**
 * Test {@link ArrowStructData} with a struct consisting of a dictionary encoded object, integer, and struct (of object
 * and dictionary encoded object).
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowComplexStructDataTest extends AbstractArrowDataTest<ArrowStructWriteData, ArrowStructReadData> {

    private static final int MAX_LENGTH = 100;

    private static final byte[][] VALUES_0 = createValues(23, 0);

    private static final byte[][] VALUES_21 = createValues(15, 1);

    private static ArrowStructDataFactory createFactory() {
        final ArrowDictEncodedObjectDataFactory<byte[]> child1 =
            new ArrowDictEncodedObjectDataFactory<>(DummyByteArraySerializer.INSTANCE);
        final ArrowIntDataFactory child2 = ArrowIntDataFactory.INSTANCE;
        final ArrowStructDataFactory child3 =
            new ArrowStructDataFactory(new ArrowObjectDataFactory<>(DummyByteArraySerializer.INSTANCE),
                new ArrowDictEncodedObjectDataFactory<>(DummyByteArraySerializer.INSTANCE));
        return new ArrowStructDataFactory(child1, child2, child3);
    }

    /** Create the test for {@link ArrowStructData} */
    public ArrowComplexStructDataTest() {
        super(createFactory());
    }

    @Override
    protected ArrowStructWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowStructWriteData);
        return (ArrowStructWriteData)o;
    }

    @Override
    protected ArrowStructReadData castR(final Object o) {
        assertTrue(o instanceof ArrowStructReadData);
        return (ArrowStructReadData)o;
    }

    @Override
    protected void setValue(final ArrowStructWriteData data, final int index, final int seed) {
        final ObjectWriteData<byte[]> data0 = data.getWriteDataAt(0);
        assertTrue(data0 instanceof ArrowDictEncodedObjectWriteData);
        final IntWriteData data1 = data.getWriteDataAt(1);
        assertTrue(data1 instanceof ArrowIntWriteData);
        final StructWriteData data2 = data.getWriteDataAt(2);
        assertTrue(data2 instanceof ArrowStructWriteData);
        final ObjectWriteData<byte[]> data20 = data2.getWriteDataAt(0);
        assertTrue(data20 instanceof ArrowObjectWriteData);
        final ObjectWriteData<byte[]> data21 = data2.getWriteDataAt(1);
        assertTrue(data21 instanceof ArrowDictEncodedObjectWriteData);

        data0.setObject(index, valueFor0(seed));
        data1.setInt(index, seed);
        data20.setObject(index, valueFor20(seed));
        data21.setObject(index, valueFor21(seed));
    }

    @Override
    protected void checkValue(final ArrowStructReadData data, final int index, final int seed) {
        final ObjectReadData<byte[]> data0 = data.getReadDataAt(0);
        assertTrue(data0 instanceof ArrowDictEncodedObjectReadData);
        final IntReadData data1 = data.getReadDataAt(1);
        assertTrue(data1 instanceof ArrowIntReadData);
        final StructReadData data2 = data.getReadDataAt(2);
        assertTrue(data2 instanceof ArrowStructReadData);
        final ObjectReadData<byte[]> data20 = data2.getReadDataAt(0);
        assertTrue(data20 instanceof ArrowObjectReadData);
        final ObjectReadData<byte[]> data21 = data2.getReadDataAt(1);
        assertTrue(data21 instanceof ArrowDictEncodedObjectReadData);

        assertArrayEquals(valueFor0(seed), data0.getObject(index));
        assertEquals(seed, data1.getInt(index));
        assertArrayEquals(valueFor20(seed), data20.getObject(index));
        assertArrayEquals(valueFor21(seed), data21.getObject(index));
    }

    @Override
    protected boolean isReleasedW(final ArrowStructWriteData data) {
        return data.m_vector == null;
    }

    @Override
    @SuppressWarnings("resource")
    protected boolean isReleasedR(final ArrowStructReadData data) {
        final ArrowStructReadData d = castR(data);
        final ArrowStructReadData d2 = d.getReadDataAt(2);
        final ArrowObjectReadData<byte[]> d20 = d2.getReadDataAt(0);

        // We just check the validity buffer of this data, of the inner struct
        // and of the 2nd level object data
        final StructVector vector = d.m_vector;
        final StructVector vector2 = d2.m_vector;
        final LargeVarBinaryVector vector20 = d20.m_vector;

        return vector.getValidityBuffer().capacity() == 0 && vector2.getValidityBuffer().capacity() == 0
            && vector20.getValidityBuffer().capacity() == 0;
    }

    @Override
    protected int getMinSize(final int valueCount, final int capacity) {
        // Number of bytes for the object data
        int numBytesObjectData = 0;
        for (int i = 0; i < valueCount; i++) {
            numBytesObjectData += new Random(i).nextInt(MAX_LENGTH);
        }

        // NB: The dictionary is not allocated
        return 6 * (int)Math.ceil(capacity / 8.0) // Validity buffers
            + capacity * 4 // Dictionary encoded data
            + capacity * 4 // Integer data
            + (capacity + 1) * 4 + numBytesObjectData // Object data (offset array + data)
            + capacity * 4; // Dictionary encoded data
    }

    private static byte[] valueFor0(final int seed) {
        final Random random = new Random(seed);
        return VALUES_0[random.nextInt(VALUES_0.length)];
    }

    private static byte[] valueFor20(final int seed) {
        final Random random = new Random(seed);
        final byte[] bytes = new byte[random.nextInt(MAX_LENGTH)];
        random.nextBytes(bytes);
        return bytes;
    }

    private static byte[] valueFor21(final int seed) {
        final Random random = new Random(seed);
        return VALUES_21[random.nextInt(VALUES_21.length)];
    }

    private static byte[][] createValues(final int numDistinct, final long seed) {
        final byte[][] values = new byte[numDistinct][];
        final Random random = new Random(seed);
        for (int i = 0; i < numDistinct; i++) {
            final byte[] bytes = new byte[random.nextInt(MAX_LENGTH)];
            random.nextBytes(bytes);
            values[i] = bytes;
        }
        return values;
    }
}
