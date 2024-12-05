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

import org.apache.arrow.memory.ArrowBuf;

/**
 * Utility class for managing Arrow offsets buffers, which map elements to their corresponding start and end indices.
 * <p>
 * For example, given offsets {@code [0, 3, 7, 10, 13]}, the elements are mapped as:
 * <ul>
 * <li>Element 0: indices 0 (inclusive) to 3 (exclusive)</li>
 * <li>Element 1: indices 3 (inclusive) to 7 (exclusive)</li>
 * <li>Element 2: indices 7 (inclusive) to 10 (exclusive)</li>
 * <li>Element 3: indices 10 (inclusive) to 13 (exclusive)</li>
 * </ul>
 * This follows the Apache Arrow specification and allows for efficient data serialization and deserialization using
 * Arrow buffers.
 * </p>
 * <p>
 * The {@code OffsetsBuffer} class provides interfaces for writing to and reading from offsets buffers, and methods to
 * create these buffers.
 * </p>
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OffsetsBuffer {

    private OffsetsBuffer() {
        // Prevent instantiation
    }

    /**
     * Represents a range of indices corresponding to a data element, with a start index (inclusive) and an end index
     * (exclusive).
     *
     * @param start start index of the data element (inclusive)
     * @param end end index of the data element (exclusive)
     */
    public record DataIndex(int start, int end) {
    }

    /**
     * Interface for writing to an offsets buffer, allowing the addition of elements with specified lengths.
     */
    public interface OffsetsWriteBuffer {

        /**
         * Adds an element at the specified index with the given length to the offsets buffer. The index must not be
         * less than the last index added. Fills up holes in the buffer with elements of length 0 if necessary.
         *
         * @param index the index at which to add the element
         * @param elementLength the length of the element
         * @return a {@code DataIndex} representing the start and end indices of the added element
         * @throws IndexOutOfBoundsException if the index is less than the last index added
         */
        DataIndex add(int index, int elementLength);

        /**
         * Closes the write buffer and returns a read buffer for accessing the offsets.
         *
         * @return an {@code OffsetsReadBuffer} for reading the offsets
         */
        OffsetsReadBuffer close();

        /**
         * Sets the number of elements in the offsets buffer, expanding or shrinking it as necessary. Fills up the
         * remaining elements with zero-length elements if the last element is not at the end of the buffer.
         *
         * @param numElements the new number of elements
         * @throws IllegalArgumentException if {@code numElements} is negative
         */
        void setNumElements(int numElements);
    }

    /**
     * Interface for reading from an offsets buffer, providing access to the start and end indices of elements.
     */
    public interface OffsetsReadBuffer {

        /**
         * Retrieves the start and end indices for the element at the specified index.
         *
         * @param index the index of the element
         * @return a {@code DataIndex} representing the start (inclusive) and end (exclusive) indices
         * @throws IndexOutOfBoundsException if the index is out of bounds
         */
        DataIndex get(int index);

        /**
         * Copies the offsets buffer data to the specified Arrow buffer.
         *
         * @param buffer the target {@code ArrowBuf} to which the offsets will be copied
         * @throws NullPointerException if {@code buffer} is null
         */
        void copyTo(ArrowBuf buffer);
    }

    /**
     * Creates a new write buffer with the specified initial number of elements.
     *
     * @param initialNumElements the initial number of elements in the buffer
     * @return an {@code OffsetsWriteBuffer} for writing offsets
     * @throws IllegalArgumentException if {@code initialNumElements} is negative
     */
    public static OffsetsWriteBuffer createWriteBuffer(final int initialNumElements) {
        // TODO implement
        return null;
    }

    /**
     * Creates a read buffer from the given Arrow buffer.
     *
     * @param buffer the {@code ArrowBuf} containing offsets data
     * @param numElements the number of elements in the buffer
     * @return an {@code OffsetsReadBuffer} for reading offsets
     * @throws NullPointerException if {@code buffer} is null
     */
    public static OffsetsReadBuffer createReadBuffer(final ArrowBuf buffer, final int numElements) {
        // TODO implement
        return null;
    }
}
