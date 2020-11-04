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
 *   Nov 4, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.knime.core.columnar.WriteData;

/**
 * Abstract implementation of {@link ArrowWriteData}. Holds a {@link FieldVector} of type F in {@link #m_vector} and an
 * offset for the indices in {@link #m_offset}. Takes care of reference counting and closes the vector when all
 * references are gone.
 *
 * Call {@link #closeWithLength(int)} to close the {@link WriteData}.
 *
 * Overwrite {@link #closeResources()} to close additional resources (make sure to call the super method).
 *
 * @param <F> the type of the {@link FieldVector}
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
abstract class AbstractArrowWriteData<F extends FieldVector> extends AbstractReferencedData implements ArrowWriteData {

    /** The vector. Use {@link #m_offset} when accessing vector data. */
    protected F m_vector;

    /** An offset describing where the values of this object start in the vector. */
    protected final int m_offset;

    /**
     * Create an abstract {@link ArrowWriteData} with the given vector and an offset of 0.
     *
     * @param vector the vector
     */
    public AbstractArrowWriteData(final F vector) {
        m_vector = vector;
        m_offset = 0;
    }

    /**
     * Create an abstract {@link ArrowWriteData} with the given vector and offset.
     *
     * @param vector the vector
     * @param offset the offset
     */
    public AbstractArrowWriteData(final F vector, final int offset) {
        m_vector = vector;
        m_offset = offset;
    }

    /**
     * Closes the {@link WriteData}. Sets the value count of the vector, checks the reference count of this object, sets
     * {@link #m_vector} to <code>null</code> and returns the vector.
     *
     * @param length the value count
     * @return the vector with the set value count
     */
    protected F closeWithLength(final int length) {
        m_vector.setValueCount(length);
        final F vector = m_vector;
        m_vector = null;
        // TODO(benjamin) is this needed
        if (getReferenceCount() != 1) {
            throw new IllegalStateException("Closed with outstanding references");
        }
        return vector;
    }

    @Override
    public void setMissing(final int index) {
        @SuppressWarnings("resource") // Validity buffer handled by vector
        final ArrowBuf validityBuffer = m_vector.getValidityBuffer();
        BitVectorHelper.unsetBit(validityBuffer, m_offset + index);
    }

    @Override
    public void expand(final int minimumCapacity) {
        while (m_vector.getValueCapacity() < minimumCapacity) {
            m_vector.reAlloc();
        }
    }

    @Override
    public int capacity() {
        return m_vector.getValueCapacity();
    }

    @Override
    protected void closeResources() {
        if (m_vector != null) {
            m_vector.close();
        }
    }

    @Override
    public String toString() {
        return ValueVectorUtility.getToString(m_vector, m_offset, m_vector.getValueCount());
    }
}
