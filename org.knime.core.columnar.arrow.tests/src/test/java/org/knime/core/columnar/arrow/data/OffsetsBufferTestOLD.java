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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class OffsetsBufferTestOLD {

    private static BufferAllocator allocator;

    @BeforeAll
    static void setUpBeforeClass() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterAll
    static void tearDownAfterClass() {
        allocator.close();
    }

    /** Tests the usedSizeFor method. */
    @Test
    void testUsedSizeFor() {
        int capacity = 10;
        int expectedSize = (capacity + 1) * Integer.BYTES;
        int actualSize = OffsetsBufferOLD.usedSizeFor(capacity);
        assertEquals(expectedSize, actualSize, "usedSizeFor should return the correct size in bytes");
    }

    /** Tests the constructor with capacity parameter. */
    @Test
    void testConstructorWithCapacity() {
        int capacity = 10;
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(capacity);
        int expectedSize = (capacity + 1) * Integer.BYTES;
        int actualSize = buffer.sizeOf();
        assertEquals(expectedSize, actualSize, "OffsetsBuffer sizeOf should return correct size after initialization");
    }

    /** Tests the constructor with offsets array parameter. */
    @Test
    void testConstructorWithOffsetsArray() {
        int[] offsetsArray = {0, 3, 7, 10, 13};
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(offsetsArray);

        int numElements = offsetsArray.length - 1;
        for (int i = 0; i < numElements; i++) {
            assertEquals(offsetsArray[i], buffer.getStartIndex(i),
                "getStartIndex should return correct value for index " + i);
            assertEquals(offsetsArray[i + 1], buffer.getEndIndex(i),
                "getEndIndex should return correct value for index " + i);
        }
    }

    /** Tests the setNumElements method. */
    @Test
    void testSetNumElements() {
        int capacity = 10;
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(capacity);

        buffer.setNumElements(5);
        int expectedSize = (5 + 1) * Integer.BYTES;
        assertEquals(expectedSize, buffer.sizeOf(), "sizeOf should return correct size after setNumElements");
    }

    /** Tests the getLastOffset method. */
    @Test
    void testGetLastOffset() {
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(5);
        assertEquals(0, buffer.getLastOffset(), "getLastOffset should return 0 initially");

        buffer.setOffsetAtIndex(0, 3); // first element size 3
        assertEquals(3, buffer.getLastOffset(), "getLastOffset should return correct value after setting first offset");

        buffer.setOffsetAtIndex(1, 4); // second element size 4
        assertEquals(7, buffer.getLastOffset(),
            "getLastOffset should return correct value after setting second offset");
    }

    /** Tests the setOffsetAtIndex method. */
    @Test
    void testSetOffsetAtIndex() {
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(5);

        int endOffset = buffer.setOffsetAtIndex(0, 3); // first element size 3
        assertEquals(3, endOffset, "setOffsetAtIndex should return correct end offset");
        assertEquals(0, buffer.getStartIndex(0), "getStartIndex should return correct value after setOffsetAtIndex");
        assertEquals(3, buffer.getEndIndex(0), "getEndIndex should return correct value after setOffsetAtIndex");

        endOffset = buffer.setOffsetAtIndex(1, 4); // second element size 4
        assertEquals(7, endOffset, "setOffsetAtIndex should return correct end offset");
        assertEquals(3, buffer.getStartIndex(1), "getStartIndex should return correct value after setOffsetAtIndex");
        assertEquals(7, buffer.getEndIndex(1), "getEndIndex should return correct value after setOffsetAtIndex");
    }

    /** Tests the setOffsetAtIndex method with gaps (holes) in indices. */
    @Test
    void testSetOffsetAtIndexWithHoles() {
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(5);

        buffer.setOffsetAtIndex(0, 3); // first element size 3

        // Skip index 1 and set at index 2
        buffer.setOffsetAtIndex(2, 5); // third element size 5

        // Now check that index 1's start and end are filled correctly
        assertEquals(3, buffer.getStartIndex(1), "getStartIndex should fill holes with last offset");
        assertEquals(3, buffer.getEndIndex(1), "getEndIndex should fill holes with last offset");

        // Check index 2
        assertEquals(3, buffer.getStartIndex(2), "getStartIndex should return correct value after setOffsetAtIndex");
        assertEquals(8, buffer.getEndIndex(2), "getEndIndex should return correct value after setOffsetAtIndex");
    }

    /** Tests the completeBuffer method. */
    @Test
    void testCompleteBuffer() {
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(5);

        buffer.setOffsetAtIndex(0, 3);
        buffer.setOffsetAtIndex(1, 4);
        buffer.completeBuffer(4); // total elements supposed to be 4

        // Indices 2 and 3 should be filled with last offset
        int lastOffset = buffer.getLastOffset(); // Should be 7
        for (int i = 2; i <= 3; i++) {
            assertEquals(lastOffset, buffer.getStartIndex(i),
                "getStartIndex should be lastOffset after completeBuffer at index " + i);
            assertEquals(lastOffset, buffer.getEndIndex(i),
                "getEndIndex should be lastOffset after completeBuffer at index " + i);
        }

        // Verify that accessing index 4 throws an exception
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStartIndex(4),
            "Accessing index 4 should throw IndexOutOfBoundsException");
    }

    /** Tests that accessing out-of-bounds indices throws IndexOutOfBoundsException. */
    @Test
    void testOutOfBoundsAccess() {
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(3);

        buffer.setOffsetAtIndex(0, 3);
        buffer.setOffsetAtIndex(1, 4);

        // Accessing valid indices should not throw exceptions
        assertDoesNotThrow(() -> buffer.getStartIndex(0), "Accessing valid start index should not throw exception");
        assertDoesNotThrow(() -> buffer.getEndIndex(0), "Accessing valid end index should not throw exception");

        // Accessing out-of-bounds indices should throw IndexOutOfBoundsException
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStartIndex(-1),
            "Accessing negative index should throw IndexOutOfBoundsException");
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getEndIndex(-1),
            "Accessing negative index should throw IndexOutOfBoundsException");
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStartIndex(2),
            "Accessing index beyond valid range should throw IndexOutOfBoundsException");
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getEndIndex(2),
            "Accessing index beyond valid range should throw IndexOutOfBoundsException");
    }

    /** Tests the copyTo and createFrom methods using an Arrow buffer. */
    @Test
    void testCopyToAndCreateFrom() {
        OffsetsBufferOLD buffer = new OffsetsBufferOLD(3);
        buffer.setOffsetAtIndex(0, 3);
        buffer.setOffsetAtIndex(1, 4);
        buffer.setOffsetAtIndex(2, 5);
        // Do not call completeBuffer to test edge case

        int numElements = 3;

        try (ArrowBuf arrowBuf = allocator.buffer(OffsetsBufferOLD.usedSizeFor(numElements))) {
            buffer.copyTo(arrowBuf);

            OffsetsBufferOLD newBuffer = OffsetsBufferOLD.createFrom(arrowBuf, numElements);

            assertOffsetsBufferEquals(buffer, newBuffer);
        }
    }

    /**
     * Utility method to compare two OffsetsBuffer instances.
     *
     * @param expected the expected OffsetsBuffer
     * @param actual the actual OffsetsBuffer
     */
    private void assertOffsetsBufferEquals(final OffsetsBufferOLD expected, final OffsetsBufferOLD actual) {
        int numElements = (expected.sizeOf() / Integer.BYTES) - 1;
        for (int i = 0; i < numElements; i++) {
            assertEquals(expected.getStartIndex(i), actual.getStartIndex(i),
                "Start index at position " + i + " should be equal");
            assertEquals(expected.getEndIndex(i), actual.getEndIndex(i),
                "End index at position " + i + " should be equal");
        }
    }
}