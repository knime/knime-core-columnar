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

import org.apache.arrow.memory.ArrowBuf;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OffsetsBuffer {

    private int[] m_offsets;

    private int m_nextIndex;

    public OffsetsBuffer(final int capacity) {
        m_offsets = new int[capacity + 1];
        m_nextIndex = 0;
    }

    public OffsetsBuffer(final int[] offsets) {
        m_offsets = offsets;
        m_nextIndex = offsets.length - 1;
    }

    public void setNumElements(final int numElements) {
        var newOffsets = new int[numElements + 1];
        System.arraycopy(m_offsets, 0, newOffsets, 0, Math.min(m_offsets.length, numElements + 1));
        m_offsets = newOffsets;

        // Update the next index if it is out of bounds
        m_nextIndex = Math.min(m_nextIndex, numElements);
    }

    public int getLastOffset() {
        return m_offsets[m_nextIndex];
    }

    public int putValue(final int index, final int size) {
        var startOffset = getLastOffset();
        var endOffset = startOffset + size;

        // Fill holes with the start offset
        for (int i = m_nextIndex + 1; i <= index; i++) {
            m_offsets[i] = startOffset;
        }
        m_nextIndex = index + 1;
        m_offsets[m_nextIndex] = endOffset;

        return endOffset;
    }

    public void endBuffer(final int length) {
        var lastOffset = getLastOffset();

        // Fill hole in the offset buffer if necessary
        for (int i = m_nextIndex + 1; i <= length; i++) {
            m_offsets[i] = lastOffset;
        }
        m_nextIndex = length;
    }

    public int getStartIndex(final int index) {
        return m_offsets[index];
    }

    public int getEndIndex(final int index) {
        return m_offsets[index + 1];
    }

    public void copyTo(final ArrowBuf offsetBuffer) {
        MemoryCopyUtils.copy(m_offsets, offsetBuffer);
    }

    public static OffsetsBuffer createFrom(final ArrowBuf offsetBuffer, final int length) {
        var offsets = new int[length + 1];
        MemoryCopyUtils.copy(offsetBuffer, offsets);
        return new OffsetsBuffer(offsets);
    }
}
