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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.knime.core.columnar.arrow.onheap.data.LargeOffsetBuffer.LargeDataIndex;
import org.knime.core.columnar.arrow.onheap.data.OffsetBuffer.DataIndex;

/**
 * Unit tests for {@link OffsetBuffer} and {@link LargeOffsetBuffer}.
 *
 * Tests are parameterized to cover both the 32-bit and 64-bit offset buffers.
 */
class OffsetBufferTest {

    private static BufferAllocator allocator;

    @BeforeAll
    static void initAllocator() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterAll
    static void closeAllocator() {
        allocator.close();
    }

    /**
     * Simple example test demonstrating how the parameterized tests are structured.
     */
    @ParameterizedTest
    @EnumSource(BufferType.class)
    void example(final BufferType bufferType) {
        // Construct a buffer with capacity for 4 elements
        var buffer = bufferType.construct(4);

        // Add an element of length 1 at index 0
        var index1 = buffer.add(0, 1);
        assertEquals(0, index1.start());
        assertEquals(1, index1.end());
    }

    /**
     * Tests adding multiple elements to ensure correct offsets and last-written index.
     */
    @ParameterizedTest
    @EnumSource(BufferType.class)
    void testAddMultipleElements(final BufferType bufferType) {
        // Capacity for 5 elements
        var buffer = bufferType.construct(5);

        // Add an element of length 3 at index 0
        var idx0 = buffer.add(0, 3);
        assertEquals(0, idx0.start());
        assertEquals(3, idx0.end());

        // Add an element of length 2 at index 1
        var idx1 = buffer.add(1, 2);
        assertEquals(3, idx1.start());
        assertEquals(5, idx1.end());

        // Add a zero-length element at index 2
        var idx2 = buffer.add(2, 0);
        assertEquals(5, idx2.start());
        assertEquals(5, idx2.end());

        // Add an element of length 5 at index 3
        var idx3 = buffer.add(3, 5);
        assertEquals(5, idx3.start());
        assertEquals(10, idx3.end());

        // Add an element of length 1 at index 4
        var idx4 = buffer.add(4, 1);
        assertEquals(10, idx4.start());
        assertEquals(11, idx4.end());

        // The last written index should now be 4
        assertEquals(4, buffer.lastWrittenIndex());
    }

    /**
     * Tests filling in "holes" with zero-length elements.
     */
    @ParameterizedTest
    @EnumSource(BufferType.class)
    void testHolesAreFilledWithZeroLength(final BufferType bufferType) {
        // Capacity for 5 elements
        var buffer = bufferType.construct(5);

        // Add an element at index 2, skipping index 0 and 1
        var idx2 = buffer.add(2, 5);
        // This should implicitly fill index 0 and 1 with zero-length offsets
        // so that start=0 for index=2.
        assertEquals(0, idx2.start(), "Start of index=2 should be 0 if holes filled with length 0");
        assertEquals(5, idx2.end());

        // Confirm index 0 and 1 were filled with zero-length
        var idx0 = buffer.get(0);
        assertEquals(0, idx0.start());
        assertEquals(0, idx0.end());
        var idx1 = buffer.get(1);
        assertEquals(0, idx1.start());
        assertEquals(0, idx1.end());

        // Add a small element at index 3
        var idx3 = buffer.add(3, 2);
        assertEquals(5, idx3.start());
        assertEquals(7, idx3.end());

        // Add a zero-length element at index 4
        var idx4 = buffer.add(4, 0);
        assertEquals(7, idx4.start());
        assertEquals(7, idx4.end());
    }

    /**
     * Tests that adding an element at an index less than the last added index throws an exception.
     */
    @ParameterizedTest
    @EnumSource(BufferType.class)
    void testAddAtDecreasingIndexThrows(final BufferType bufferType) {
        var buffer = bufferType.construct(3);

        // Add at index 0 and 1
        buffer.add(0, 2);
        buffer.add(1, 2);

        // Attempt to add at index 0 again (this is not allowed)
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.add(0, 1), "Should not allow overwriting index 0");
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.add(1, 1), "Should not allow overwriting index 1");

        // Assert that we can still add elements after the last
        buffer.add(2, 1);
    }

    /**
     * Tests the behavior of setNumElements to shrink and then expand the buffer, ensuring that existing offsets are
     * preserved and that new space is zeroed out.
     */
    @ParameterizedTest
    @EnumSource(BufferType.class)
    void testSetNumElements(final BufferType bufferType) {
        var buffer = bufferType.construct(5);

        // Add an element of length 2 at index 0
        var idx0 = buffer.add(0, 2);
        assertEquals(0, idx0.start());
        assertEquals(2, idx0.end());

        // Add an element of length 3 at index 1
        var idx1 = buffer.add(1, 3);
        assertEquals(2, idx1.start());
        assertEquals(5, idx1.end());

        // Shrink to 2 elements total
        buffer.setNumElements(2);
        assertEquals(1, buffer.lastWrittenIndex(), "Last written index should be 1 after shrinking to 2 elements");
        // Attempting to get index 2 now should fail
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(2));

        // Expand back to 4
        buffer.setNumElements(4);
        // The existing offsets for index 0 and 1 should remain
        assertEquals(2, buffer.get(0).end());
        assertEquals(5, buffer.get(1).end());

        // We should be able to add a new element at index 2 and 3 now
        var idx2 = buffer.add(2, 0);
        assertEquals(5, idx2.start());
        assertEquals(5, idx2.end());
        var idx3 = buffer.add(3, 2);
        assertEquals(5, idx3.start());
        assertEquals(7, idx3.end());
    }

    /**
     * Tests that fillWithZeroLength zeros out all elements after the last added.
     */
    @ParameterizedTest
    @EnumSource(BufferType.class)
    void testFillWithZeroLengthAfterAdds(final BufferType bufferType) {
        var buffer = bufferType.construct(5);
        buffer.add(0, 2); // [0..2]
        buffer.add(1, 1); // [2..3]
        // We have last index = 1, so everything after 1 is not yet set.

        // Now fill with zero length from index=2..4
        buffer.fillWithZeroLength();

        // Check index 2..4
        for (int i = 2; i < 5; i++) {
            var idx = buffer.get(i);
            assertEquals(3, idx.start(), "All subsequent start indices should be 3");
            assertEquals(3, idx.end(), "All subsequent end indices should be 3");
        }
    }

    /**
     * Tests reading and writing offsets to an ArrowBuf round-trip: create, add data, copy out, createFrom again.
     */
    @ParameterizedTest
    @EnumSource(BufferType.class)
    void testCopyToAndCreateFromArrowBuf(final BufferType bufferType) {
        var buffer = bufferType.construct(5);

        buffer.add(0, 2); // [0..2]
        buffer.add(1, 3); // [2..5]
        buffer.fillWithZeroLength(); // fill indices 2..4

        // Use usedSizeFor() to figure out how large the ArrowBuf must be
        int requiredBytes = (bufferType == BufferType.OFFSET_BUFFER_32_BIT) ? OffsetBuffer.usedSizeFor(5)
            : LargeOffsetBuffer.usedSizeFor(5);

        try (ArrowBuf arrowBuf = allocator.buffer(requiredBytes)) {
            // Copy the buffer data to arrowBuf
            buffer.copyTo(arrowBuf);

            // Now create a new wrapper from arrowBuf
            var recreated = bufferType.createFrom(arrowBuf, 5);

            // Check that the offsets match
            assertEquals(0, recreated.get(0).start());
            assertEquals(2, recreated.get(0).end());
            assertEquals(2, recreated.get(1).start());
            assertEquals(5, recreated.get(1).end());
            // Everything else should start at 5 and end at 5
            for (int i = 2; i < 5; i++) {
                var idx = recreated.get(i);
                assertEquals(5, idx.start());
                assertEquals(5, idx.end());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(BufferType.class)
    void testCreateFromArrowBufWithZeroElements(final BufferType bufferType) {
        try (ArrowBuf arrowBuf = allocator.buffer(0)) {
            var buffer = bufferType.createFrom(arrowBuf, 0);
            assertEquals(0, buffer.getNumData(0));
            assertThrows(IndexOutOfBoundsException.class, () -> buffer.get(0));
        }
    }

    @ParameterizedTest
    @EnumSource(BufferType.class)
    void testGetNumData(final BufferType bufferType) {
        var buffer = bufferType.construct(5);

        // Add an element of length 2 at index 0 => offsets [0..2]
        buffer.add(0, 2);
        // Add an element of length 3 at index 1 => offsets [2..5]
        buffer.add(1, 3);

        // After two elements (indices 0 and 1), the highest offset is 5
        assertEquals(5, buffer.getNumData(2), "Data end offset after 2 elements should be 5");

        // We haven't added anything to indices 2..4 yet, so they are zero-length by default.
        // Asking for 5 total elements shouldn't change the end offset.
        assertEquals(5, buffer.getNumData(5),
            "Data end offset after 5 elements is still 5 because indices 2..4 are zero-length");

        // Add an element at index 2 with length 4 => offsets [5..9]
        buffer.add(2, 4);

        // Now after 3 elements, the highest offset should be 9
        assertEquals(9, buffer.getNumData(3), "Data end offset after 3 elements should be 9");

        // Indices 3 and 4 are still zero-length
        assertEquals(9, buffer.getNumData(5),
            "Data end offset after 5 elements is 9 because indices 3..4 are zero-length");
    }

    @Test
    void testOffsetBufferSizeOf() {
        // capacity = 0 => m_offsets.length = 1 => sizeOf() = 1 * Integer.BYTES + Integer.BYTES = 4 + 4 = 8
        OffsetBuffer buffer0 = new OffsetBuffer(0);
        assertEquals(8, buffer0.sizeOf(), "sizeOf() for an empty OffsetBuffer(0) should be 8 bytes");

        // capacity = 5 => m_offsets.length = 6 => sizeOf() = 6 * Integer.BYTES + Integer.BYTES = 24 + 4 = 28
        OffsetBuffer buffer5 = new OffsetBuffer(5);
        assertEquals(28, buffer5.sizeOf(), "sizeOf() for OffsetBuffer(5) should be 28 bytes");
    }

    @Test
    void testLargeOffsetBufferSizeOf() {
        // capacity = 0 => m_offsets.length = 1 => sizeOf() = 1 * Long.BYTES + Integer.BYTES = 8 + 4 = 12
        LargeOffsetBuffer largeBuffer0 = new LargeOffsetBuffer(0);
        assertEquals(12, largeBuffer0.sizeOf(), "sizeOf() for an empty LargeOffsetBuffer(0) should be 12 bytes");

        // capacity = 5 => m_offsets.length = 6 => sizeOf() = 6 * Long.BYTES + Integer.BYTES = 48 + 4 = 52
        LargeOffsetBuffer largeBuffer5 = new LargeOffsetBuffer(5);
        assertEquals(52, largeBuffer5.sizeOf(), "sizeOf() for LargeOffsetBuffer(5) should be 52 bytes");
    }

    /**
     * Enum representing which buffer type to construct in the parameterized tests.
     */
    private enum BufferType {
            OFFSET_BUFFER_32_BIT, OFFSET_BUFFER_64_BIT;

        /** Constructs an instance of {@link OffsetBuffer} or {@link LargeOffsetBuffer}. */
        OffsetBufferWrapper construct(final int capacity) {
            return switch (this) {
                case OFFSET_BUFFER_32_BIT -> new OffsetBufferWrapper(new OffsetBuffer(capacity));
                case OFFSET_BUFFER_64_BIT -> new OffsetBufferWrapper(new LargeOffsetBuffer(capacity));
            };
        }

        /** Creates an instance of {@link OffsetBuffer} or {@link LargeOffsetBuffer} from an {@link ArrowBuf}. */
        OffsetBufferWrapper createFrom(final ArrowBuf buffer, final int numElements) {
            return switch (this) {
                case OFFSET_BUFFER_32_BIT -> new OffsetBufferWrapper(OffsetBuffer.createFrom(buffer, numElements));
                case OFFSET_BUFFER_64_BIT -> new OffsetBufferWrapper(LargeOffsetBuffer.createFrom(buffer, numElements));
            };
        }
    }

    /**
     * Wraps either {@link OffsetBuffer} or {@link LargeOffsetBuffer} to test both implementations with a unified
     * interface.
     */
    private static final class OffsetBufferWrapper {

        private static DataIndex i(final LargeDataIndex index) {
            return new DataIndex((int)index.start(), (int)index.end());
        }

        private final OffsetBuffer m_buffer;

        private final LargeOffsetBuffer m_largeOffsetBuffer;

        private OffsetBufferWrapper(final OffsetBuffer buffer) {
            m_buffer = buffer;
            m_largeOffsetBuffer = null;
        }

        private OffsetBufferWrapper(final LargeOffsetBuffer buffer) {
            m_buffer = null;
            m_largeOffsetBuffer = buffer;
        }

        public int lastWrittenIndex() {
            if (m_buffer != null) {
                return m_buffer.lastWrittenIndex();
            } else {
                return m_largeOffsetBuffer.lastWrittenIndex();
            }
        }

        public DataIndex add(final int index, final int elementLength) {
            if (m_buffer != null) {
                return m_buffer.add(index, elementLength);
            } else {
                return i(m_largeOffsetBuffer.add(index, elementLength));
            }
        }

        public void fillWithZeroLength() {
            if (m_buffer != null) {
                m_buffer.fillWithZeroLength();
            } else {
                m_largeOffsetBuffer.fillWithZeroLength();
            }
        }

        public int getNumData(final int numElements) {
            if (m_buffer != null) {
                return m_buffer.getNumData(numElements);
            } else {
                long result = m_largeOffsetBuffer.getNumData(numElements);
                // For consistency in test, cast down if we only need int
                // (In production, you might keep it as long.)
                return (int)result;
            }
        }

        public DataIndex get(final int index) {
            if (m_buffer != null) {
                return m_buffer.get(index);
            } else {
                return i(m_largeOffsetBuffer.get(index));
            }
        }

        public void setNumElements(final int numElements) {
            if (m_buffer != null) {
                m_buffer.setNumElements(numElements);
            } else {
                m_largeOffsetBuffer.setNumElements(numElements);
            }
        }

        public void copyTo(final ArrowBuf arrowBuf) {
            if (m_buffer != null) {
                m_buffer.copyTo(arrowBuf);
            } else {
                m_largeOffsetBuffer.copyTo(arrowBuf);
            }
        }
    }
}
