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
 *   Sep 20, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVectorHelper;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class ValidityBuffer {

    private byte[] m_validity;

    public static long usedSizeFor(final int capacity) {
        return BitVectorHelper.getValidityBufferSize(capacity);
    }

    /**
     * Creates a new validity buffer with the given capacity.
     *
     * @param capacity the capacity of the validity buffer
     */
    public ValidityBuffer(final int capacity) {
        m_validity = new byte[BitVectorHelper.getValidityBufferSize(capacity)];
    }

    private ValidityBuffer(final byte[] validity) {
        m_validity = validity;
    }

    /**
     * Sets the validity of the value at the given index.
     *
     * @param index the index of the value
     * @param value the validity of the value
     */
    public void set(final int index, final boolean value) {
        var byteIndex = BitVectorHelper.byteIndex(index);
        var bitIndex = BitVectorHelper.bitIndex(index);

        // TODO copied from BitVectorHelper.setValidityBit

        // the byte is promoted to an int, because according to Java specification,
        // bytes will be promoted to ints automatically, upon expression evaluation.
        // by promoting it manually, we avoid the unnecessary conversions.
        int currentByte = m_validity[byteIndex];
        final int bitMask = 1 << bitIndex;
        if (value) {
            currentByte |= bitMask;
        } else {
            currentByte &= ~bitMask;
        }

        m_validity[byteIndex] = (byte)currentByte;
    }

    /**
     * @param index the index of the value
     * @return the validity of the value at the given index
     */
    public boolean isSet(final int index) {
        var byteIndex = BitVectorHelper.byteIndex(index);
        var bitIndex = BitVectorHelper.bitIndex(index);
        return ((m_validity[byteIndex] >> bitIndex) & 0x01) == 1;
    }

    /**
     * Expands or shrinks the validity buffer to the given capacity.
     *
     * @param numElements the new capacity
     */
    public void setNumElements(final int numElements) {
        var newValidity = new byte[BitVectorHelper.getValidityBufferSize(numElements)];
        System.arraycopy(m_validity, 0, newValidity, 0, Math.min(newValidity.length, m_validity.length));
        m_validity = newValidity;
    }

    /**
     * @return the size of the validity buffer in bytes
     */
    public long sizeOf() {
        return m_validity.length;
    }

    public void copyTo(final ArrowBuf validityBuffer) {
        validityBuffer.setBytes(0, m_validity, 0, m_validity.length);
    }

    public static ValidityBuffer createFrom(final ArrowBuf validityBuffer, final int length) {
        var numBytes = BitVectorHelper.getValidityBufferSize(length);
        var validity = new byte[numBytes];
        validityBuffer.getBytes(0, validity, 0, numBytes);
        return new ValidityBuffer(validity);
    }
}