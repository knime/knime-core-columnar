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
 *   Sep 30, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.apache.arrow.vector.IntVector;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedObjectData.ArrowDictEncodedObjectDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedObjectData.ArrowDictEncodedObjectReadData;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedObjectData.ArrowDictEncodedObjectWriteData;

/**
 * Test {@link ArrowDictEncodedObjectData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowDictEncodedObjectDataTest
    extends AbstractArrowDataTest<ArrowDictEncodedObjectWriteData<byte[]>, ArrowDictEncodedObjectReadData<byte[]>> {

    private static final int NUM_DISTINCT = 23;

    private static final int MAX_LENGTH = 100;

    private static final byte[][] VALUES = createValues();

    /** Create the test for {@link ArrowDictEncodedObjectData} */
    public ArrowDictEncodedObjectDataTest() {
        super(new ArrowDictEncodedObjectDataFactory<byte[]>(DummyByteArraySerializer.INSTANCE));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ArrowDictEncodedObjectWriteData<byte[]> castW(final Object o) {
        assertTrue(o instanceof ArrowDictEncodedObjectWriteData);
        return (ArrowDictEncodedObjectWriteData<byte[]>)o;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ArrowDictEncodedObjectReadData<byte[]> castR(final Object o) {
        assertTrue(o instanceof ArrowDictEncodedObjectReadData);
        return (ArrowDictEncodedObjectReadData<byte[]>)o;
    }

    @Override
    protected void setValue(final ArrowDictEncodedObjectWriteData<byte[]> data, final int index, final int seed) {
        data.setObject(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowDictEncodedObjectReadData<byte[]> data, final int index, final int seed) {
        assertArrayEquals(valueFor(seed), data.getObject(index));
    }

    @Override
    protected boolean isReleasedW(final ArrowDictEncodedObjectWriteData<byte[]> data) {
        return data.m_vector == null;
    }

    @Override
    @SuppressWarnings("resource") // Resources handled by vector
    protected boolean isReleasedR(final ArrowDictEncodedObjectReadData<byte[]> data) {
        // TODO check for the dictionary??
        final IntVector v = castR(data).m_vector;
        return v.getDataBuffer().capacity() == 0 && v.getValidityBuffer().capacity() == 0;
    }

    @Override
    protected int getMinSize(final int valueCount, final int capacity) {
        // NB: The dictionary is not allocated
        return 4 * capacity // 4 bytes per value for data
            + (int)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer
    }

    @Override
    @SuppressWarnings({"resource"}) // Resource handled by data object
    public void testSizeOf() {
        // Super method does not allocate memory for dictionary
        super.testSizeOf();

        // Write each value of the dictionary
        final int numValues = NUM_DISTINCT;
        final ArrowDictEncodedObjectWriteData<byte[]> writeData = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            writeData.setObject(i, VALUES[i]);
        }
        final ArrowDictEncodedObjectReadData<byte[]> readData = writeData.close(numValues);

        // Allocate the dictionary
        readData.getDictionary();

        final int expectedSize = getMinSize(numValues, numValues) // Index vector
            + Arrays.stream(VALUES).mapToInt(v -> v.length).sum() // dictionary data buffer
            + 4 * numValues // dictionary offset buffer
            + (int)Math.ceil(numValues / 8.0); // dictionary validity buffer
        assertTrue("Size to small. Got " + readData.sizeOf() + ", expected >= " + expectedSize,
            readData.sizeOf() >= expectedSize);

        readData.release();
    }

    @Override
    @SuppressWarnings("resource")
    public void testToString() {
        // Super method does not test with allocated dictionary
        super.testToString();

        // Write each value of the dictionary
        final int numValues = NUM_DISTINCT;
        final ArrowDictEncodedObjectWriteData<byte[]> writeData = createWrite(numValues);
        for (int i = 0; i < numValues; i++) {
            writeData.setObject(i, VALUES[i]);
        }

        String s = writeData.toString();
        assertNotNull(s);
        assertFalse(s.isEmpty());

        final ArrowDictEncodedObjectReadData<byte[]> readData = writeData.close(numValues);

        // Fill the dictionary
        readData.getDictionary();

        s = readData.toString();
        assertNotNull(s);
        assertFalse(s.isEmpty());

        readData.release();
    }

    private static byte[] valueFor(final int seed) {
        return VALUES[new Random(seed).nextInt(NUM_DISTINCT)];
    }

    private static byte[][] createValues() {
        final byte[][] values = new byte[NUM_DISTINCT][];
        final Random random = new Random(10);
        for (int i = 0; i < NUM_DISTINCT; i++) {
            final byte[] bytes = new byte[random.nextInt(MAX_LENGTH)];
            random.nextBytes(bytes);
            values[i] = bytes;
        }
        return values;
    }
}
