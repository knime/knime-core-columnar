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
 */
package org.knime.core.columnar.arrow.onheap.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ValidityBuffer}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
class ValidityBufferTest {

    private static BufferAllocator allocator;

    @BeforeAll
    static void initAllocator() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterAll
    static void closeAllocator() {
        allocator.close();
    }

    @Test
    void testConstructorWithZeroCapacity() {
        // Create a ValidityBuffer with 0 capacity
        ValidityBuffer buffer = new ValidityBuffer(0);
        assertEquals(0, buffer.sizeOf(), "Buffer of capacity 0 should have size 0 in bytes.");
    }

    @Test
    void testSetAndIsSetBasic() {
        int capacity = 10; // 10 bits
        ValidityBuffer buffer = new ValidityBuffer(capacity);

        // Initially, all bits should be unset (false)
        for (int i = 0; i < capacity; i++) {
            assertFalse(buffer.isSet(i), "Bit " + i + " should initially be unset.");
        }

        // Set some bits
        buffer.set(0, true); // first bit
        buffer.set(3, true);
        buffer.set(9, true); // last bit

        assertTrue(buffer.isSet(0), "Bit 0 should be set.");
        assertTrue(buffer.isSet(3), "Bit 3 should be set.");
        assertTrue(buffer.isSet(9), "Bit 9 should be set.");
        assertFalse(buffer.isSet(1), "Bit 1 should be unset.");
        assertFalse(buffer.isSet(2), "Bit 2 should be unset.");
        assertFalse(buffer.isSet(4), "Bit 4 should be unset.");
    }

    @Test
    void testSetUnsetAgain() {
        int capacity = 8;
        ValidityBuffer buffer = new ValidityBuffer(capacity);

        // Set all bits to true
        for (int i = 0; i < capacity; i++) {
            buffer.set(i, true);
            assertTrue(buffer.isSet(i), "Bit " + i + " should be set.");
        }

        // Unset all bits again
        for (int i = 0; i < capacity; i++) {
            buffer.set(i, false);
            assertFalse(buffer.isSet(i), "Bit " + i + " should be unset after clearing.");
        }
    }

    @Test
    void testResizeExpand() {
        // Start with smaller capacity
        ValidityBuffer buffer = new ValidityBuffer(5);
        buffer.set(0, true);
        buffer.set(4, true);

        // Expand capacity
        buffer.setNumElements(16); // new capacity for 16 bits
        assertEquals(ValidityBuffer.usedSizeFor(16), buffer.sizeOf(), "Size in bytes should match usedSizeFor(16).");

        // Previously set bits should remain
        assertTrue(buffer.isSet(0), "Previously set bit 0 should remain set after expansion.");
        assertTrue(buffer.isSet(4), "Previously set bit 4 should remain set after expansion.");

        // New bits should default to unset
        for (int i = 5; i < 16; i++) {
            assertFalse(buffer.isSet(i), "Newly added bit " + i + " should be unset by default.");
        }
    }

    @Test
    void testResizeShrink() {
        // Start with larger capacity
        ValidityBuffer buffer = new ValidityBuffer(16);
        buffer.set(0, true);
        buffer.set(10, true);
        buffer.set(15, true);

        // Shrink capacity
        buffer.setNumElements(8); // new capacity for only 8 bits
        assertEquals(ValidityBuffer.usedSizeFor(8), buffer.sizeOf(), "Size in bytes should match usedSizeFor(8).");

        // Bits beyond the new capacity are lost
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> buffer.isSet(10),
            "Bit 10 should no longer exist after shrinking.");
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> buffer.isSet(15),
            "Bit 15 should no longer exist after shrinking.");

        // The lower bits remain
        assertTrue(buffer.isSet(0), "Bit 0 should remain set after shrinking.");
    }

    @Test
    void testSetFromMultipleBuffers() {
        // Prepare multiple buffers
        ValidityBuffer buffer1 = new ValidityBuffer(8);
        buffer1.set(0, true);
        buffer1.set(3, true);

        ValidityBuffer buffer2 = new ValidityBuffer(8);
        buffer2.set(1, true);
        buffer2.set(3, true);
        buffer2.set(5, true);

        ValidityBuffer buffer3 = new ValidityBuffer(8);
        // Intentionally leave some bits unset in buffer3

        // Target buffer
        ValidityBuffer combined = new ValidityBuffer(8);
        combined.setFrom(buffer1, buffer2, null, buffer3);
        // 'null' is treated as "all bits unset"

        // The resulting buffer is the OR of [buffer1, buffer2, buffer3]
        // buffer1 bits: 0, 3
        // buffer2 bits: 1, 3, 5
        // buffer3 bits: none
        // combined bits: 0, 1, 3, 5
        assertTrue(combined.isSet(0));
        assertTrue(combined.isSet(1));
        assertTrue(combined.isSet(3));
        assertTrue(combined.isSet(5));
        assertFalse(combined.isSet(2));
        assertFalse(combined.isSet(4));
        assertFalse(combined.isSet(6));
        assertFalse(combined.isSet(7));
    }

    @Test
    void testCreateFromArrowBuf() {
        // Prepare an ArrowBuf with 2 bytes
        try (ArrowBuf arrowBuf = allocator.buffer(2)) {
            // Write some pattern: let's set bits 0 and 7
            arrowBuf.setByte(0, 0b10000001); // sets bit 0 and bit 7
            arrowBuf.setByte(1, 0b00000000);

            // Create from the buffer, telling it we have 16 bits capacity
            ValidityBuffer buffer = ValidityBuffer.createFrom(arrowBuf, 16);
            assertEquals(ValidityBuffer.usedSizeFor(16), buffer.sizeOf(),
                "ValidityBuffer should have used size for 16 bits (2 bytes).");

            // Check bits
            // byte 0: bit 0 (LSB) = 1, bit 7 (MSB) = 1
            // byte 1: all 0
            // Indexes: bit 0 => isSet(0) == true, bit 7 => isSet(7) == true
            assertTrue(buffer.isSet(0), "Bit 0 should be set from arrowBuf.");
            assertTrue(buffer.isSet(7), "Bit 7 should be set from arrowBuf.");
            for (int i = 1; i < 7; i++) {
                assertFalse(buffer.isSet(i), "Bit " + i + " should be unset.");
            }
            for (int i = 8; i < 16; i++) {
                assertFalse(buffer.isSet(i), "Bit " + i + " should be unset.");
            }
        }
    }

    @Test
    void testCopyToArrowBuf() {
        ValidityBuffer buffer = new ValidityBuffer(9);
        buffer.set(0, true);
        buffer.set(7, true);

        try (ArrowBuf arrowBuf = allocator.buffer(2)) {
            buffer.copyTo(arrowBuf);

            // Verify that arrowBuf now has the same bit pattern
            // bit 0 and bit 7 => 0b10000001 => 129 decimal
            assertEquals(0b10000001, arrowBuf.getByte(0) & 0xFF);
            assertEquals(0b00000000, arrowBuf.getByte(1) & 0xFF);
        }
    }

    @Test
    void testUsedSizeFor() {
        // For 1 bit, we need 1 byte
        assertEquals(1, ValidityBuffer.usedSizeFor(1));
        // For 8 bits, we need 1 byte
        assertEquals(1, ValidityBuffer.usedSizeFor(8));
        // For 9 bits, we need 2 bytes
        assertEquals(2, ValidityBuffer.usedSizeFor(9));
        // For 16 bits, we need 2 bytes
        assertEquals(2, ValidityBuffer.usedSizeFor(16));
        // For 17 bits, we need 3 bytes
        assertEquals(3, ValidityBuffer.usedSizeFor(17));
    }

    @Test
    void testToString() {
        ValidityBuffer buffer = new ValidityBuffer(5);
        buffer.set(0, true);
        buffer.set(2, true);
        // The representation will be something like "[1, 0, 1, 0, 0]"
        String representation = buffer.toString();
        assertTrue(representation.contains("1, 0, 1, 0, 0"), "ToString() should reflect the set/unset bits in order.");
    }
}
