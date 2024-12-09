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
 *   Dec 5, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import java.util.Objects;

import org.apache.arrow.memory.ArrowBuf;

/**
 * Utility class for managing Arrow offsets buffers.
 * <P>
 * For example, given offsets {@code [0, 3, 7, 10, 13]}, the elements are mapped as:
 * <ul>
 * <li>Element 0: indices 0 (inclusive) to 3 (exclusive)</li>
 * <li>Element 1: indices 3 (inclusive) to 7 (exclusive)</li>
 * <li>Element 2: indices 7 (inclusive) to 10 (exclusive)</li>
 * <li>Element 3: indices 10 (inclusive) to 13 (exclusive)</li>
 * </ul>
 * <P>
 * This follows the Apache Arrow specification and allows for efficient data serialization and deserialization using
 * Arrow buffers.
 * <P>
 * The {@code OffsetsBuffer} class provides classes for writing to and reading from offsets buffers, and methods to
 * create these buffers, for both int and long based offsets.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OffsetsBuffer {

    private OffsetsBuffer() {
        // Prevent instantiation
    }

    // --------------------------------------------------------------------------------------------
    // Int-based offsets
    // --------------------------------------------------------------------------------------------

    /**
     * Represents a range of indices corresponding to a data element, with a start index (inclusive) and an end index
     * (exclusive) using int offsets.
     *
     * @param start start index of the data element (inclusive)
     * @param end end index of the data element (exclusive)
     */
    public record IntDataIndex(int start, int end) {
    }

    /**
     * Class for writing to an int-based offsets buffer, allowing the addition of elements with specified lengths.
     */
    public static final class IntOffsetsWriteBuffer {

        private int[] m_offsets;

        private int m_numElements;

        private int m_lastIndexAdded = -1;

        private int m_currentOffset = 0;

        private IntOffsetsWriteBuffer(final int initialNumElements) {
            if (initialNumElements < 0) {
                throw new IllegalArgumentException("initialNumElements cannot be negative");
            }
            this.m_numElements = initialNumElements;
            // Offsets array has length numElements + 1
            this.m_offsets = new int[m_numElements + 1];
            this.m_offsets[0] = 0; // Initial offset
        }

        /**
         * Adds an element at the specified index with the given length to the offsets buffer. The index must not be
         * less than the last index added. Fills up holes in the buffer with elements of length 0 if necessary.
         *
         * @param index the index at which to add the element
         * @param elementLength the length of the element
         * @return a {@code IntDataIndex} representing the start and end indices of the added element
         * @throws IndexOutOfBoundsException if the index is less than the last index added
         */
        public IntDataIndex add(final int index, final int elementLength) {
            if (index < m_lastIndexAdded) {
                throw new IndexOutOfBoundsException("Index cannot be less than the last index added");
            }
            if (index >= m_numElements) {
                setNumElements(index + 1);
            }

            // Fill any holes with zero-length elements
            for (int i = m_lastIndexAdded + 1; i < index; i++) {
                m_offsets[i + 1] = m_currentOffset; // Zero-length element
            }

            // Add the element
            m_currentOffset += elementLength;
            m_offsets[index + 1] = m_currentOffset;
            m_lastIndexAdded = index;

            return new IntDataIndex(m_offsets[index], m_offsets[index + 1]);
        }

        /**
         * Sets the number of elements in the offsets buffer, expanding or shrinking it as necessary. Fills up the
         * remaining elements with zero-length elements if the last element is not at the end of the buffer.
         *
         * @param numElements the new number of elements
         * @throws IllegalArgumentException if {@code numElements} is negative
         */
        public void setNumElements(final int numElements) {
            if (numElements < 0) {
                throw new IllegalArgumentException("numElements cannot be negative");
            }
            if (numElements != this.m_numElements) {
                int[] newOffsets = new int[numElements + 1];
                int copyLength = Math.min(this.m_numElements + 1, numElements + 1);
                System.arraycopy(this.m_offsets, 0, newOffsets, 0, copyLength);

                // If expanding, initialize new elements
                if (numElements > this.m_numElements) {
                    int lastOffset = m_offsets[this.m_numElements];
                    for (int i = this.m_numElements + 1; i <= numElements; i++) {
                        newOffsets[i] = lastOffset;
                    }
                }

                this.m_offsets = newOffsets;
                this.m_numElements = numElements;
            }
        }

        /**
         * Closes the write buffer and returns a read buffer for accessing the offsets.
         *
         * @return an {@code IntOffsetsReadBuffer} for reading the offsets
         */
        public IntOffsetsReadBuffer close() {
            // Fill any remaining elements with zero-length elements
            for (int i = m_lastIndexAdded + 1; i < m_numElements; i++) {
                m_offsets[i + 1] = m_currentOffset; // Zero-length
            }
            return new IntOffsetsReadBuffer(m_offsets, m_numElements);
        }

        /**
         * Returns the start index of the element at the specified index.
         *
         * @param index the index of the element
         * @return the start index of the element
         */
        public int getStartIndex(final int index) {
            Objects.checkIndex(index, m_lastIndexAdded + 1);
            return m_offsets[index];
        }
    }

    /**
     * Class for reading from an int-based offsets buffer, providing access to the start and end indices of elements.
     */
    public static final class IntOffsetsReadBuffer {

        private final int[] m_offsets;

        private final int m_numElements;

        /** Constructor for internal use (from write buffer) */
        private IntOffsetsReadBuffer(final int[] offsets, final int numElements) {
            this.m_offsets = offsets;
            this.m_numElements = numElements;
        }

        /** Constructor for creating from ArrowBuf */
        @SuppressWarnings("resource")
        private IntOffsetsReadBuffer(final ArrowBuf buffer, final int numElements) {
            Objects.requireNonNull(buffer, "buffer cannot be null");
            if (numElements < 0) {
                throw new IllegalArgumentException("numElements cannot be negative");
            }
            this.m_numElements = numElements;
            this.m_offsets = new int[numElements + 1];
            // Copy data from ArrowBuf to offsets array
            MemoryCopyUtils.copy(buffer, m_offsets);
        }

        /**
         * Retrieves the start and end indices for the element at the specified index.
         *
         * @param index the index of the element
         * @return a {@code IntDataIndex} representing the start (inclusive) and end (exclusive) indices
         * @throws IndexOutOfBoundsException if the index is out of bounds
         */
        public IntDataIndex get(final int index) {
            Objects.checkIndex(index, m_numElements);
            int start = m_offsets[index];
            int end = m_offsets[index + 1];
            return new IntDataIndex(start, end);
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
    }

    /**
     * Creates a new int-based write buffer with the specified initial number of elements.
     *
     * @param initialNumElements the initial number of elements in the buffer
     * @return an {@code IntOffsetsWriteBuffer} for writing int-based offsets
     * @throws IllegalArgumentException if {@code initialNumElements} is negative
     */
    public static IntOffsetsWriteBuffer createIntWriteBuffer(final int initialNumElements) {
        return new IntOffsetsWriteBuffer(initialNumElements);
    }

    /**
     * Creates an int-based read buffer from the given Arrow buffer.
     *
     * @param buffer the {@code ArrowBuf} containing offsets data
     * @param numElements the number of elements in the buffer
     * @return an {@code IntOffsetsReadBuffer} for reading int-based offsets
     * @throws NullPointerException if {@code buffer} is null
     */
    public static IntOffsetsReadBuffer createIntReadBuffer(final ArrowBuf buffer, final int numElements) {
        return new IntOffsetsReadBuffer(buffer, numElements);
    }

    /**
     * Computes the size in bytes required to store the int-based offsets for a given capacity if they are serialized to
     * an Arrow buffer.
     *
     * @param capacity the number of elements
     * @return the size in bytes required
     */
    public static int usedIntSizeFor(final int capacity) {
        return (capacity + 1) * Integer.BYTES;
    }

    // --------------------------------------------------------------------------------------------
    // Long-based offsets
    // --------------------------------------------------------------------------------------------

    /**
     * Represents a range of indices corresponding to a data element, with a start index (inclusive) and an end index
     * (exclusive) using long offsets.
     *
     * @param start start index of the data element (inclusive)
     * @param end end index of the data element (exclusive)
     */
    public record LongDataIndex(long start, long end) {
    }

    /**
     * Class for writing to a long-based offsets buffer, allowing the addition of elements with specified lengths.
     */
    public static final class LongOffsetsWriteBuffer {

        private long[] m_offsets;

        private int m_numElements;

        private int m_lastIndexAdded = -1;

        private long m_currentOffset = 0;

        private LongOffsetsWriteBuffer(final int initialNumElements) {
            if (initialNumElements < 0) {
                throw new IllegalArgumentException("initialNumElements cannot be negative");
            }
            this.m_numElements = initialNumElements;
            // Offsets array has length numElements + 1
            this.m_offsets = new long[m_numElements + 1];
            this.m_offsets[0] = 0L; // Initial offset
        }

        /**
         * Adds an element at the specified index with the given length to the offsets buffer. The index must not be
         * less than the last index added. Fills up holes in the buffer with elements of length 0 if necessary.
         *
         * @param index the index at which to add the element
         * @param elementLength the length of the element
         * @return a {@code LongDataIndex} representing the start and end indices of the added element
         * @throws IndexOutOfBoundsException if the index is less than the last index added
         */
        public LongDataIndex add(final int index, final long elementLength) {
            if (index < m_lastIndexAdded) {
                throw new IndexOutOfBoundsException("Index cannot be less than the last index added");
            }
            if (index >= m_numElements) {
                setNumElements(index + 1);
            }

            // Fill any holes with zero-length elements
            for (int i = m_lastIndexAdded + 1; i < index; i++) {
                m_offsets[i + 1] = m_currentOffset; // Zero-length element
            }

            // Add the element
            m_currentOffset += elementLength;
            m_offsets[index + 1] = m_currentOffset;
            m_lastIndexAdded = index;

            return new LongDataIndex(m_offsets[index], m_offsets[index + 1]);
        }

        /**
         * Sets the number of elements in the offsets buffer, expanding or shrinking it as necessary. Fills up the
         * remaining elements with zero-length elements if the last element is not at the end of the buffer.
         *
         * @param numElements the new number of elements
         * @throws IllegalArgumentException if {@code numElements} is negative
         */
        public void setNumElements(final int numElements) {
            if (numElements < 0) {
                throw new IllegalArgumentException("numElements cannot be negative");
            }
            if (numElements != this.m_numElements) {
                long[] newOffsets = new long[numElements + 1];
                int copyLength = Math.min(this.m_numElements + 1, numElements + 1);
                System.arraycopy(this.m_offsets, 0, newOffsets, 0, copyLength);

                // If expanding, initialize new elements
                if (numElements > this.m_numElements) {
                    long lastOffset = m_offsets[this.m_numElements];
                    for (int i = this.m_numElements + 1; i <= numElements; i++) {
                        newOffsets[i] = lastOffset;
                    }
                }

                this.m_offsets = newOffsets;
                this.m_numElements = numElements;
            }
        }

        /**
         * Closes the write buffer and returns a read buffer for accessing the offsets.
         *
         * @return a {@code LongOffsetsReadBuffer} for reading the offsets
         */
        public LongOffsetsReadBuffer close() {
            // Fill any remaining elements with zero-length elements
            for (int i = m_lastIndexAdded + 1; i < m_numElements; i++) {
                m_offsets[i + 1] = m_currentOffset; // Zero-length
            }
            return new LongOffsetsReadBuffer(m_offsets, m_numElements);
        }

        /**
         * Returns the start index of the element at the specified index.
         *
         * @param index the index of the element
         * @return the start index of the element
         */
        public long getStartIndex(final int index) {
            // TODO this is strange logic
            var idx = Math.max(Math.min(index, m_lastIndexAdded + 1), 0);
            return m_offsets[idx];
        }
    }

    /**
     * Class for reading from a long-based offsets buffer, providing access to the start and end indices of elements.
     */
    public static final class LongOffsetsReadBuffer {

        private final long[] m_offsets;

        private final int m_numElements;

        /** Constructor for internal use (from write buffer) */
        private LongOffsetsReadBuffer(final long[] offsets, final int numElements) {
            this.m_offsets = offsets;
            this.m_numElements = numElements;
        }

        /** Constructor for creating from ArrowBuf */
        @SuppressWarnings("resource")
        private LongOffsetsReadBuffer(final ArrowBuf buffer, final int numElements) {
            Objects.requireNonNull(buffer, "buffer cannot be null");
            if (numElements < 0) {
                throw new IllegalArgumentException("numElements cannot be negative");
            }
            this.m_numElements = numElements;
            this.m_offsets = new long[numElements + 1];
            MemoryCopyUtils.copy(buffer, m_offsets);
        }

        /**
         * Retrieves the start and end indices for the element at the specified index.
         *
         * @param index the index of the element
         * @return a {@code LongDataIndex} representing the start (inclusive) and end (exclusive) indices
         * @throws IndexOutOfBoundsException if the index is out of bounds
         */
        public LongDataIndex get(final int index) {
            Objects.checkIndex(index, m_numElements);
            long start = m_offsets[index];
            long end = m_offsets[index + 1];
            return new LongDataIndex(start, end);
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
    }

    /**
     * Creates a new long-based write buffer with the specified initial number of elements.
     *
     * @param initialNumElements the initial number of elements in the buffer
     * @return a {@code LongOffsetsWriteBuffer} for writing long-based offsets
     * @throws IllegalArgumentException if {@code initialNumElements} is negative
     */
    public static LongOffsetsWriteBuffer createLongWriteBuffer(final int initialNumElements) {
        return new LongOffsetsWriteBuffer(initialNumElements);
    }

    /**
     * Creates a long-based read buffer from the given Arrow buffer.
     *
     * @param buffer the {@code ArrowBuf} containing offsets data
     * @param numElements the number of elements in the buffer
     * @return a {@code LongOffsetsReadBuffer} for reading long-based offsets
     * @throws NullPointerException if {@code buffer} is null
     */
    public static LongOffsetsReadBuffer createLongReadBuffer(final ArrowBuf buffer, final int numElements) {
        return new LongOffsetsReadBuffer(buffer, numElements);
    }

    /**
     * Computes the size in bytes required to store the long-based offsets for a given capacity if they are serialized
     * to an Arrow buffer.
     *
     * @param capacity the number of elements
     * @return the size in bytes required
     */
    public static int usedLongSizeFor(final int capacity) {
        return (capacity + 1) * Long.BYTES;
    }
}
