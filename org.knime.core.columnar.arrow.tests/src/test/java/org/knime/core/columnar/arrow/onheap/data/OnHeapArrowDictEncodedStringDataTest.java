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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.arrow.onheap.AbstractOnHeapArrowDataTest;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedStringData.ArrowDictEncodedStringDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedStringData.ArrowDictEncodedStringReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedStringData.ArrowDictEncodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictKeys;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DefaultDataTraits;

/**
 * Test {@link OnHeapArrowDictEncodedStringData}
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class OnHeapArrowDictEncodedStringDataTest
    extends AbstractOnHeapArrowDataTest<ArrowDictEncodedStringWriteData<Long>, ArrowDictEncodedStringReadData<Long>> {

    private static final int MAX_LENGTH = 100;

    private static final int NUM_DIFFERENT_STRINGS = 20;

    private static final Map<Integer, String> VALUES = new HashMap<>();

    /** Create the test for {@link OnHeapArrowDictEncodedStringData} */
    public OnHeapArrowDictEncodedStringDataTest() {
        super(new ArrowDictEncodedStringDataFactory(new DefaultDataTraits(new DictEncodingTrait(KeyType.LONG_KEY))));
    }

    @Override
    protected ArrowDictEncodedStringWriteData<Long> createWrite(final int numValues) {
        final var data = super.createWrite(numValues);
        data.setKeyGenerator(DictKeys.createAscendingKeyGenerator(KeyType.LONG_KEY));
        return data;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ArrowDictEncodedStringWriteData<Long> castW(final Object o) {
        assertTrue(o instanceof ArrowDictEncodedStringWriteData);
        return (ArrowDictEncodedStringWriteData<Long>)o;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ArrowDictEncodedStringReadData<Long> castR(final Object o) {
        assertTrue(o instanceof ArrowDictEncodedStringReadData);
        return (ArrowDictEncodedStringReadData<Long>)o;
    }

    @Override
    protected void setValue(final ArrowDictEncodedStringWriteData<Long> data, final int index, final int seed) {
        data.setString(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowDictEncodedStringReadData<Long> data, final int index, final int seed) {
        assertEquals(valueFor(seed), data.getString(index));
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        var countedStrings = new HashSet<String>(NUM_DIFFERENT_STRINGS);
        long numBytes = 0;
        for (int i = 0; i < valueCount; i++) {
            var value = valueFor(i);
            if (countedStrings.add(value)) {
                numBytes += value.length() * 2; // UTF-16
            }
        }
        return numBytes // value data buffer
            + capacity * 8 // key data buffer
            + 3 * (long)Math.ceil(capacity / 8.0); // validity buffers of struct, key, value
    }

    @Override
    protected long getMinSizeW(final int valueCount, final int capacity) {
        return getMinSize(valueCount, capacity) //
            + 8 * capacity; // size tracking buffer
    }

    private static String valueFor(final int seed) {
        final Random random = new Random(seed);
        final int key = random.nextInt(NUM_DIFFERENT_STRINGS);

        return VALUES.computeIfAbsent(key, s -> {
            final int count = random.nextInt(MAX_LENGTH);
            return RandomStringUtils.random(count, 0, Integer.MAX_VALUE, true, true, null, random);
        });
    }

    /** Test reading data of a slice, where the dictionary values are stored outside of the slice */
    @Test
    public void testReadSliceDictLutWithValueOutsideOfSlice() {
        final int numValues = 32;
        final int sliceStart = 5;
        final int sliceLength = 10;
        final String testString = "adsfghjk";

        // Write outside to inside of the slice.
        // Value should only be stored at position 0 outside of slice
        final var writeData = createWrite(numValues);
        for (int i = 0; i < sliceStart + sliceLength; i++) {
            writeData.setString(i, testString);
        }

        // Read only the slice. The value should still be found
        final var readData = castR(writeData.close(numValues));
        final var slicedRead = castR(readData.slice(sliceStart, sliceLength));
        for (int i = 0; i < sliceLength; i++) {
            assertFalse(slicedRead.isMissing(i));
            assertEquals(slicedRead.getString(i), testString);
        }

        readData.release();
    }

    /** Test writing into a slice, using keys from the original vector */
    @Test
    public void testWriteSliceDictLutAndUseKeyOutside() {
        final int numValues = 32;
        final int sliceStart = 5;
        final int sliceLength = 10;
        final String testString = "adsfghjk";

        // Write outside to inside of the slice.
        // Value should only be stored at position 0 outside of slice
        final var writeData = createWrite(numValues);
        for (int i = 0; i < sliceStart; i++) {
            writeData.setString(i, testString);
        }

        final var slicedWrite = writeData.slice(sliceStart);
        for (int i = 0; i < sliceLength; i++) {
            slicedWrite.setString(i, testString);
        }

        // Read full data
        final var readData = castR(writeData.close(numValues));
        for (int i = 0; i < numValues; i++) {
            if (i < sliceStart + sliceLength) {
                assertFalse(readData.isMissing(i));
                assertEquals(testString, readData.getString(i));
                assertEquals((long)0, (long)readData.getDictKey(i));
            } else {
                assertTrue(readData.isMissing(i));
            }
        }

        readData.release();
    }
}
