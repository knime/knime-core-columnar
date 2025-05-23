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
 *   Jan 17, 2025 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import java.util.Arrays;
import java.util.Objects;

import org.apache.arrow.memory.ArrowBuf;

/**
 * On-heap implementation of an Arrow offset buffer. The buffer is used to write offsets for data elements in a variable
 * sized vector to an Arrow buffer. This implementation uses 32-bit signed integers to represent the offsets. Use
 * {@link LargeOffsetBuffer} for 64-bit signed offsets.
 * <p>
 * For example, given offsets {@code [0, 3, 7, 10, 13]}, the elements are mapped as:
 * <ul>
 * <li>Element 0: indices 0 (inclusive) to 3 (exclusive)</li>
 * <li>Element 1: indices 3 (inclusive) to 7 (exclusive)</li>
 * <li>Element 2: indices 7 (inclusive) to 10 (exclusive)</li>
 * <li>Element 3: indices 10 (inclusive) to 13 (exclusive)</li>
 * </ul>
 * <p>
 * This follows the Apache Arrow specification and allows for efficient data serialization and deserialization using
 * Arrow buffers.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 * @see LargeOffsetBuffer
 */
public final class OffsetBuffer {

    /**
     * Represents a range of indices corresponding to a data element, with a start index (inclusive) and an end index
     * (exclusive) using int offsets.
     *
     * @param start start index of the data element (inclusive)
     * @param end end index of the data element (exclusive)
     */
    public record DataIndex(int start, int end) {

        /**
         * @return the length of the data element (<code>end - start</code>)
         */
        public int length() {
            return end - start;
        }
    }

    private int[] m_offsets;

    private int m_lastIndexAdded;

    /**
     * Creates an int-based read buffer from the given Arrow buffer.
     *
     * @param buffer the {@code ArrowBuf} containing offsets data
     * @param numElements the number of elements in the buffer
     * @return an {@code IntOffsetsBuffer} for reading int-based offsets
     * @throws NullPointerException if {@code buffer} is null
     * @throws IllegalArgumentException if {@code numElements} is negative
     */
    public static OffsetBuffer createFrom(final ArrowBuf buffer, final int numElements) {
        if (numElements == 0) {
            // Note: For zero elements, Arrow saves an empty buffer.
            // The logic in the else case would still try to read one value from the offsets buffer.
            return new OffsetBuffer(0);
        } else {
            var offsets = new int[numElements + 1];
            MemoryCopyUtils.copy(buffer, offsets);
            return new OffsetBuffer(offsets);
        }
    }

    /**
     * Computes the size in bytes required to store the int-based offsets for a given capacity if they are serialized to
     * an Arrow buffer.
     *
     * @param capacity the number of elements
     * @return the size in bytes required
     */
    public static int usedSizeFor(final int capacity) {
        return (capacity + 1) * Integer.BYTES;
    }

    /**
     * Creates a new int-based write buffer with the specified initial number of elements.
     *
     * @param initialNumElements the initial number of elements in the buffer
     * @throws IllegalArgumentException if {@code initialNumElements} is negative
     */
    public OffsetBuffer(final int initialNumElements) {
        if (initialNumElements < 0) {
            throw new IllegalArgumentException("initialNumElements cannot be negative");
        }
        m_offsets = new int[initialNumElements + 1];
        m_lastIndexAdded = -1;
    }

    private OffsetBuffer(final int[] offsets) {
        m_offsets = offsets;
        m_lastIndexAdded = offsets.length - 2;
    }

    /**
     * @return the number of elements that can be stored in this offsets buffer
     */
    private int numElements() {
        return m_offsets.length - 1;
    }

    /**
     * Sets the element length of all elements before <code>toIdx</code> to 0.
     *
     * @param toIdx the index up to which to set the element length to 0 (exclusive)
     */
    private void fillWithZeroLength(final int toIdx) {
        for (int i = m_lastIndexAdded + 1; i < toIdx; i++) {
            m_offsets[i + 1] = m_offsets[i]; // Zero-length element
        }
        m_lastIndexAdded = toIdx - 1;
    }

    /**
     * @return the last index that was successfully added
     */
    public int lastWrittenIndex() {
        return m_lastIndexAdded;
    }

    /**
     * @return the size of the offset buffer in bytes
     */
    public long sizeOf() {
        return (long)m_offsets.length * Integer.BYTES + Integer.BYTES;
    }

    /**
     * Adds an element at the specified index with the given length to the offsets buffer. The index must larger than
     * the last index added. Fills up holes in the buffer with elements of length 0 if necessary.
     *
     * @param index the index at which to add the element
     * @param elementLength the length of the element
     * @return a {@code IntDataIndex} representing the start and end indices of the added element
     * @throws IndexOutOfBoundsException if the index is not larger than the last index added or greater than the
     *             capacity
     */
    public DataIndex add(final int index, final int elementLength) {
        if (index <= m_lastIndexAdded) {
            throw new IndexOutOfBoundsException("Index must be larger than the last index added");
        }
        Objects.checkIndex(index, numElements());

        // Fill any holes with zero-length elements
        fillWithZeroLength(index);

        // Add the element
        m_offsets[index + 1] = m_offsets[index] + elementLength;
        m_lastIndexAdded = index;

        return new DataIndex(m_offsets[index], m_offsets[index + 1]);
    }

    /**
     * Sets the element length of all elements after the last added element to 0.
     */
    public void fillWithZeroLength() {
        fillWithZeroLength(numElements());
    }

    /**
     * @param numElements the number of elements
     * @return the end index of all data if the number of elements is numElements
     */
    public int getNumData(final int numElements) {
        var idx = Math.min(numElements - 1, m_lastIndexAdded);
        return m_offsets[idx + 1]; // end index
    }

    /**
     * Retrieves the start and end indices for the element at the specified index.
     *
     * @param index the index of the element
     * @return a {@code DataIndex} representing the start (inclusive) and end (exclusive) indices
     * @throws IndexOutOfBoundsException if the index is out of bounds
     */
    public DataIndex get(final int index) {
        Objects.checkIndex(index, m_lastIndexAdded + 1);
        int start = m_offsets[index];
        int end = m_offsets[index + 1];
        return new DataIndex(start, end);
    }

    /**
     * Expand or shrink the data to the given size. Keeps the existing data up to the new size.
     *
     * @param numElements the new size of the data
     */
    public void setNumElements(final int numElements) {
        if (numElements < 0) {
            throw new IllegalArgumentException("numElements cannot be negative");
        }
        if (numElements != numElements()) {
            var newOffsets = new int[numElements + 1];
            var copyLength = Math.min(newOffsets.length, m_lastIndexAdded + 2);
            System.arraycopy(m_offsets, 0, newOffsets, 0, copyLength);
            m_offsets = newOffsets;
            m_lastIndexAdded = Math.min(m_lastIndexAdded, numElements - 1);
        }
    }

    /**
     * Copies the offsets buffer data to the specified Arrow buffer.
     *
     * @param buffer the target {@code ArrowBuf} to which the offsets will be copied
     * @throws NullPointerException if {@code buffer} is null
     */
    @SuppressWarnings("resource")
    public void copyTo(final ArrowBuf buffer) {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        MemoryCopyUtils.copy(m_offsets, buffer);
    }

    @Override
    public String toString() {
        return "IntOffsetsBuffer{" + "m_offsets=" + Arrays.toString(m_offsets) + ", m_lastIndexAdded="
            + m_lastIndexAdded + '}';
    }
}