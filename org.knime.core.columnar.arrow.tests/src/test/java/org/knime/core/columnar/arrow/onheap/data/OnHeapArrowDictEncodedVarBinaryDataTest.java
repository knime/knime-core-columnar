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
package org.knime.core.columnar.arrow.onheap.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.knime.core.columnar.arrow.onheap.AbstractOnHeapArrowDataTest;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictKeys;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

/**
 * Test {@link OnHeapArrowDictEncodedVarBinaryData}
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class OnHeapArrowDictEncodedVarBinaryDataTest extends
    AbstractOnHeapArrowDataTest<ArrowDictEncodedVarBinaryWriteData<Byte>, ArrowDictEncodedVarBinaryReadData<Byte>> {

    private static final int MAX_LENGTH = 100;

    private static final Map<Integer, Object> VALUES = new HashMap<>();

    private static final int NUM_DIFFERENT_VALUES = 20;

    /** Create the test for {@link OnHeapArrowDictEncodedVarBinaryData} */
    public OnHeapArrowDictEncodedVarBinaryDataTest() {
        super(ArrowDictEncodedVarBinaryDataFactory.getInstance(KeyType.BYTE_KEY));
    }

    @Override
    protected ArrowDictEncodedVarBinaryWriteData<Byte> createWrite(final int numValues) {
        final var data = super.createWrite(numValues);
        data.setKeyGenerator(DictKeys.createAscendingKeyGenerator(KeyType.BYTE_KEY));
        return data;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ArrowDictEncodedVarBinaryWriteData<Byte> castW(final Object o) {
        assertTrue(o instanceof ArrowDictEncodedVarBinaryWriteData);
        return (ArrowDictEncodedVarBinaryWriteData<Byte>)o;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ArrowDictEncodedVarBinaryReadData<Byte> castR(final Object o) {
        assertTrue(o instanceof ArrowDictEncodedVarBinaryReadData);
        return (ArrowDictEncodedVarBinaryReadData<Byte>)o;
    }

    @Override
    protected void setValue(final ArrowDictEncodedVarBinaryWriteData<Byte> data, final int index, final int seed) {
        data.setBytes(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowDictEncodedVarBinaryReadData<Byte> data, final int index, final int seed) {
        assertArrayEquals(valueFor(seed), data.getBytes(index));
    }

    private static byte[] valueFor(final int seed) {
        final Random random = new Random(seed);
        final int key = random.nextInt(NUM_DIFFERENT_VALUES);

        return (byte[])VALUES.computeIfAbsent(key, s -> {
            final byte[] bytes = new byte[random.nextInt(MAX_LENGTH)];
            random.nextBytes(bytes);
            return bytes;
        });
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        var countedValues = new HashSet<byte[]>(NUM_DIFFERENT_VALUES);
        long numBytes = 0;
        for (int i = 0; i < valueCount; i++) {
            var value = valueFor(i);
            if (countedValues.add(value)) {
                numBytes += value.length;
            }
        }
        return numBytes + 8 * capacity // value data buffer with offset
            + capacity // key data buffer (byte keys)
            + 3 * (long)Math.ceil(capacity / 8.0); // validity buffers of struct, key, value
    }

    /** Test reading data of a slice, where the dictionary values are stored outside of the slice */
    @Test
    public void testSliceDictLut() {
        final int numValues = 32;
        final int sliceStart = 5;
        final int sliceLength = 10;
        Random random = new Random(numValues);
        final byte[] testData = new byte[64];
        random.nextBytes(testData);

        // Write outside to inside of the slice.
        // Value should only be stored at position 0 outside of slice
        final var writeData = createWrite(numValues);
        for (int i = 0; i < sliceStart + sliceLength; i++) {
            writeData.setBytes(i, testData);
        }

        // Read only the slice. The value should still be found
        final var readData = castR(writeData.close(numValues));
        final var slicedRead = castR(readData.slice(sliceStart, sliceLength));
        for (int i = 0; i < sliceLength; i++) {
            assertFalse(slicedRead.isMissing(i));
            assertTrue(Arrays.equals(slicedRead.getBytes(i), testData));
        }

        readData.release();
    }

    /** Test writing into a slice, using keys from the original vector */
    @Test
    public void testWriteSliceDictLutAndUseKeyOutside() {
        final int numValues = 32;
        final int sliceStart = 5;
        final int sliceLength = 10;
        Random random = new Random(numValues);
        final byte[] testData = new byte[64];
        random.nextBytes(testData);

        // Write outside to inside of the slice.
        // Value should only be stored at position 0 outside of slice
        final var writeData = createWrite(numValues);
        for (int i = 0; i < sliceStart; i++) {
            writeData.setBytes(i, testData);
        }

        final var slicedWrite = writeData.slice(sliceStart);
        for (int i = 0; i < sliceLength; i++) {
            slicedWrite.setBytes(i, testData);
        }

        // Read full data
        final var readData = castR(writeData.close(numValues));
        for (int i = 0; i < numValues; i++) {
            if (i < sliceStart + sliceLength) {
                assertFalse(readData.isMissing(i));
                assertTrue(Arrays.equals(testData, readData.getBytes(i)));
                assertEquals(0, (byte)readData.getDictKey(i));
            } else {
                assertTrue(readData.isMissing(i));
            }
        }

        readData.release();
    }
}
