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
 *   Dec 4, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link OffsetsBuffer} class.
 */
class OffsetsBufferTest {

    private static RootAllocator allocator;

    @BeforeAll
    static void setupAllocator() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterAll
    static void closeAllocator() {
        allocator.close();
    }

    /**
     * Tests the creation of a write buffer with a negative initial number of elements.
     */
    @Test
    void testCreateWriteBufferWithNegativeInitialElements() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            OffsetsBuffer.createWriteBuffer(-1);
        }, "Expected IllegalArgumentException for negative initialNumElements");

        assertEquals("initialNumElements cannot be negative", exception.getMessage(),
            "Exception message should indicate negative initialNumElements");
    }

    /**
     * Tests the creation of a read buffer with a null ArrowBuf.
     */
    @Test
    void testCreateReadBufferWithNullArrowBuf() {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> {
            OffsetsBuffer.createReadBuffer(null, 0);
        }, "Expected NullPointerException when buffer is null");

        assertEquals("buffer cannot be null", exception.getMessage(),
            "Exception message should indicate that buffer is null");
    }

    /**
     * Tests adding elements to the write buffer and retrieving them from the read buffer.
     */
    @Test
    void testAddAndRetrieveOffsets() {
        // Using createAndPopulateBuffer utility method
        OffsetsBuffer.OffsetsReadBuffer readBuffer = createAndPopulateBuffer(3, 4, 2);

        // Verify elements
        OffsetsBuffer.DataIndex index0 = readBuffer.get(0);
        assertEquals(0, index0.start(), "Element 0 start index should be 0");
        assertEquals(3, index0.end(), "Element 0 end index should be 3");

        OffsetsBuffer.DataIndex index1 = readBuffer.get(1);
        assertEquals(3, index1.start(), "Element 1 start index should be 3");
        assertEquals(7, index1.end(), "Element 1 end index should be 7");

        OffsetsBuffer.DataIndex index2 = readBuffer.get(2);
        assertEquals(7, index2.start(), "Element 2 start index should be 7");
        assertEquals(9, index2.end(), "Element 2 end index should be 9");
    }

    /**
     * Tests adding elements out of order and expects an IndexOutOfBoundsException.
     */
    @Test
    void testAddWithInvalidIndexOrder() {
        OffsetsBuffer.OffsetsWriteBuffer writeBuffer = OffsetsBuffer.createWriteBuffer(0);

        writeBuffer.add(0, 3);
        IndexOutOfBoundsException exception = assertThrows(IndexOutOfBoundsException.class, () -> {
            writeBuffer.add(-1, 4);
        }, "Expected IndexOutOfBoundsException for index less than last index added");

        assertEquals("Index cannot be less than the last index added", exception.getMessage(),
            "Exception message should indicate invalid index order");
    }

    /**
     * Tests setting a negative number of elements in the write buffer.
     */
    @Test
    void testSetNumElementsNegative() {
        OffsetsBuffer.OffsetsWriteBuffer writeBuffer = OffsetsBuffer.createWriteBuffer(0);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            writeBuffer.setNumElements(-5);
        }, "Expected IllegalArgumentException for negative numElements");

        assertEquals("numElements cannot be negative", exception.getMessage(),
            "Exception message should indicate negative numElements");
    }

    /**
     * Tests the behavior when retrieving an element with an out-of-bounds index from the read buffer.
     */
    @Test
    void testGetWithOutOfBoundsIndex() {
        OffsetsBuffer.OffsetsReadBuffer readBuffer = createAndPopulateBuffer(3, 4);

        IndexOutOfBoundsException exception = assertThrows(IndexOutOfBoundsException.class, () -> {
            readBuffer.get(5);
        }, "Expected IndexOutOfBoundsException for out-of-bounds index");

        assertEquals("Index 5 is out of bounds", exception.getMessage(),
            "Exception message should indicate out-of-bounds index");
    }

    /**
     * Tests copying offsets to an ArrowBuf and verifies the copied data.
     */
    @Test
    void testCopyToArrowBuf() {
        OffsetsBuffer.OffsetsReadBuffer readBuffer = createAndPopulateBuffer(3, 4, 2);

        int numElements = 3;
        int bufferSize = (numElements + 1) * Integer.BYTES;

        try (ArrowBuf arrowBuf = allocator.buffer(bufferSize)) {
            readBuffer.copyTo(arrowBuf);

            assertEquals(0, arrowBuf.getInt(0), "Offset at position 0 should be 0");
            assertEquals(3, arrowBuf.getInt(4), "Offset at position 1 should be 3");
            assertEquals(7, arrowBuf.getInt(8), "Offset at position 2 should be 7");
            assertEquals(9, arrowBuf.getInt(12), "Offset at position 3 should be 9");
        }
    }

    /**
     * Tests copying offsets to a null ArrowBuf and expects a NullPointerException.
     */
    @Test
    void testCopyToNullArrowBuf() {
        OffsetsBuffer.OffsetsReadBuffer readBuffer = createAndPopulateBuffer();

        NullPointerException exception = assertThrows(NullPointerException.class, () -> {
            readBuffer.copyTo(null);
        }, "Expected NullPointerException when copying to null buffer");

        assertEquals("buffer cannot be null", exception.getMessage(),
            "Exception message should indicate that buffer is null");
    }

    /**
     * Tests filling up holes in the write buffer with zero-length elements.
     */
    @Test
    void testFillHolesWithZeroLengthElements() {
        OffsetsBuffer.OffsetsWriteBuffer writeBuffer = OffsetsBuffer.createWriteBuffer(5);
        writeBuffer.add(0, 3);
        writeBuffer.add(2, 4); // Index 1 should be filled with zero-length element
        writeBuffer.add(4, 2); // Index 3 should be filled with zero-length element

        OffsetsBuffer.OffsetsReadBuffer readBuffer = writeBuffer.close();

        OffsetsBuffer.DataIndex index1 = readBuffer.get(1);
        assertEquals(3, index1.start(), "Element 1 start index should be 3");
        assertEquals(3, index1.end(), "Element 1 end index should be 3 (zero-length)");

        OffsetsBuffer.DataIndex index3 = readBuffer.get(3);
        assertEquals(7, index3.start(), "Element 3 start index should be 7");
        assertEquals(7, index3.end(), "Element 3 end index should be 7 (zero-length)");
    }

    /**
     * Tests that setNumElements can expand the buffer with existing values.
     */
    @Test
    void testSetNumElementsExpandBuffer() {
        OffsetsBuffer.OffsetsWriteBuffer writeBuffer = OffsetsBuffer.createWriteBuffer(2);
        writeBuffer.add(0, 3);
        writeBuffer.add(1, 4);

        writeBuffer.setNumElements(4); // Expand to 4 elements

        writeBuffer.add(2, 2);
        writeBuffer.add(3, 1);

        OffsetsBuffer.OffsetsReadBuffer readBuffer = writeBuffer.close();

        OffsetsBuffer.DataIndex index3 = readBuffer.get(3);
        assertEquals(9, index3.start(), "Element 3 start index should be 9");
        assertEquals(10, index3.end(), "Element 3 end index should be 10");
    }

    /**
     * Tests that setNumElements can shrink the buffer with existing values.
     */
    @Test
    void testSetNumElementsShrinkBuffer() {
        OffsetsBuffer.OffsetsWriteBuffer writeBuffer = OffsetsBuffer.createWriteBuffer(5);
        writeBuffer.add(0, 3);
        writeBuffer.add(1, 4);
        writeBuffer.add(2, 2);
        writeBuffer.add(3, 1);
        writeBuffer.add(4, 5);

        writeBuffer.setNumElements(3); // Shrink to 3 elements

        OffsetsBuffer.OffsetsReadBuffer readBuffer = writeBuffer.close();

        IndexOutOfBoundsException exception = assertThrows(IndexOutOfBoundsException.class, () -> {
            readBuffer.get(4);
        }, "Expected IndexOutOfBoundsException for index beyond the new numElements");

        assertEquals("Index 4 is out of bounds", exception.getMessage(),
            "Exception message should indicate out-of-bounds index");
    }

    /**
     * Tests that close fills up remaining elements with zero-length elements.
     */
    @Test
    void testCloseFillsRemainingElements() {
        OffsetsBuffer.OffsetsWriteBuffer writeBuffer = OffsetsBuffer.createWriteBuffer(5);
        writeBuffer.add(0, 3);
        writeBuffer.add(2, 4);

        // Close without adding elements at index 1, 3, 4
        OffsetsBuffer.OffsetsReadBuffer readBuffer = writeBuffer.close();

        OffsetsBuffer.DataIndex index1 = readBuffer.get(1);
        assertEquals(3, index1.start(), "Element 1 start index should be 3");
        assertEquals(3, index1.end(), "Element 1 end index should be 3 (zero-length)");

        OffsetsBuffer.DataIndex index3 = readBuffer.get(3);
        assertEquals(7, index3.start(), "Element 3 start index should be 7");
        assertEquals(7, index3.end(), "Element 3 end index should be 7 (zero-length)");

        OffsetsBuffer.DataIndex index4 = readBuffer.get(4);
        assertEquals(7, index4.start(), "Element 4 start index should be 7");
        assertEquals(7, index4.end(), "Element 4 end index should be 7 (zero-length)");
    }

    /**
     * Utility method to create a write buffer and add elements with specified lengths.
     *
     * @param lengths the lengths of the elements to add
     * @return the closed read buffer after adding elements
     */
    private static OffsetsBuffer.OffsetsReadBuffer createAndPopulateBuffer(final int... lengths) {
        int numElements = lengths.length;
        OffsetsBuffer.OffsetsWriteBuffer writeBuffer = OffsetsBuffer.createWriteBuffer(numElements);
        for (int i = 0; i < lengths.length; i++) {
            writeBuffer.add(i, lengths[i]);
        }
        return writeBuffer.close();
    }
}

