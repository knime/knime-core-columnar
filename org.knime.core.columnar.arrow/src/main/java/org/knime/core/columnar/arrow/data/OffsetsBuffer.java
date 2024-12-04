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
 *   Dec 3, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import java.util.Arrays;

import org.apache.arrow.memory.ArrowBuf;

/**
 * Holds the offsets of elements in a buffer or array.
 * <P>
 * For example, the offsets {@code [0, 3, 7, 10, 13]} indicate:
 * <ul>
 * <li>Element 0 is at indices 0 to 3 (exclusive of 3)</li>
 * <li>Element 1 is at indices 3 to 7 (exclusive of 7)</li>
 * <li>Element 2 is at indices 7 to 10 (exclusive of 10)</li>
 * <li>Element 3 is at indices 10 to 13 (exclusive of 13)</li>
 * </ul>
 * <P>
 * This is in line with the Arrow specification and can be copied to and from an Arrow buffer.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class OffsetsBuffer {

    private int[] m_offsets;

    private int m_nextIndex;

    /**
     * Computes the size in bytes required to store the offsets for a given capacity.
     *
     * @param capacity the number of elements
     * @return the size in bytes required
     */
    public static int usedSizeFor(final int capacity) {
        return (capacity + 1) * Integer.BYTES;
    }

    /**
     * Constructs an {@code OffsetsBuffer} with the specified capacity.
     *
     * @param capacity the number of elements
     */
    public OffsetsBuffer(final int capacity) {
        m_offsets = new int[capacity + 1];
        m_nextIndex = 0;
    }

    /**
     * Constructs an {@code OffsetsBuffer} from an existing offsets array.
     *
     * @param offsets the offsets array
     */
    OffsetsBuffer(final int[] offsets) {
        this.m_offsets = Arrays.copyOf(offsets, offsets.length);
        this.m_nextIndex = offsets.length - 1;
    }

    /**
     * Sets the number of elements and resizes the offsets array accordingly.
     *
     * @param numElements the new number of elements
     */
    public void setNumElements(final int numElements) {
        m_offsets = Arrays.copyOf(m_offsets, numElements + 1);
        m_nextIndex = Math.min(m_nextIndex, numElements);
    }

    /**
     * Returns the last offset value.
     *
     * @return the last offset
     */
    public int getLastOffset() {
        return m_offsets[m_nextIndex];
    }

    /**
     * Updates the offsets array at the specified index with the given size.
     *
     * @param index the index at which to set the offset
     * @param size the size of the value
     * @return the end offset after inserting the value
     */
    public int setOffsetAtIndex(final int index, final int size) {
        int startOffset = getLastOffset();
        int endOffset = startOffset + size;

        // Fill holes with the start offset
        Arrays.fill(m_offsets, m_nextIndex + 1, index + 1, startOffset);

        m_nextIndex = index + 1;
        m_offsets[m_nextIndex] = endOffset;

        return endOffset;
    }

    /**
     * Completes the buffer by filling any remaining offsets up to the specified length.
     *
     * @param length the total number of elements
     */
    public void completeBuffer(final int length) {
        int lastOffset = getLastOffset();

        // Fill holes in the offset buffer if necessary
        Arrays.fill(m_offsets, m_nextIndex + 1, length + 1, lastOffset);
        m_nextIndex = length;
    }

    /**
     * Returns the start index for the specified element index.
     *
     * @param index the element index
     * @return the start offset
     */
    public int getStartIndex(final int index) {
        return m_offsets[index];
    }

    /**
     * Returns the end index for the specified element index.
     *
     * @param index the element index
     * @return the end offset
     */
    public int getEndIndex(final int index) {
        return m_offsets[index + 1];
    }

    /**
     * Returns the size in bytes of the offsets buffer.
     *
     * @return the size in bytes
     */
    public int sizeOf() {
        return m_offsets.length * Integer.BYTES;
    }

    /**
     * Copies the offsets to the specified Arrow buffer.
     *
     * @param offsetBuffer the target Arrow buffer
     */
    public void copyTo(final ArrowBuf offsetBuffer) {
        MemoryCopyUtils.copy(m_offsets, offsetBuffer);
    }

    /**
     * Creates an {@code OffsetsBuffer} from an Arrow buffer.
     *
     * @param offsetBuffer the source Arrow buffer
     * @param length the number of elements
     * @return a new {@code OffsetsBuffer} initialized from the Arrow buffer
     */
    public static OffsetsBuffer createFrom(final ArrowBuf offsetBuffer, final int length) {
        int[] offsets = new int[length + 1];
        MemoryCopyUtils.copy(offsetBuffer, offsets);
        return new OffsetsBuffer(offsets);
    }
}
