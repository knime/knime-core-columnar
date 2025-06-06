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
package org.knime.core.columnar.arrow.onheap.data;

import org.knime.core.columnar.arrow.data.AbstractReferencedData;

/**
 * Abstract implementation of {@link OnHeapArrowReadData}. Holds a generic {@link #m_data data object}, the
 * {@link #m_validity validity buffer}, the {@link #m_offset offset}, and the {@link #m_length length} of the data.
 *
 * Sets the data object and the validity buffer to <code>null</code> when the reference count reaches 0. Override
 * {@link #closeResources()} and call <code>super.closeResources()</code> to clean up additional resources.
 *
 * @param <T> the type of the data
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
abstract class AbstractOnHeapArrowReadData<T> extends AbstractReferencedData implements OnHeapArrowReadData {

    /** The data of this object. */
    protected T m_data;

    /** The validity buffer of this data object. */
    protected ValidityBuffer m_validity;

    /** An offset describing where the values of this object start in the vector. */
    protected final int m_offset;

    /** The length of the data */
    protected final int m_length;

    /**
     * Create an abstract {@link OnHeapArrowReadData} with the given vector, an offset of 0 and the length of the
     * vector.
     *
     * @param vector the vector
     * @param missingValues if all, no or some values are missing
     */
    public AbstractOnHeapArrowReadData(final T data, final ValidityBuffer validity, final int length) {
        this(data, validity, 0, length);
    }

    /**
     * Create an abstract {@link OnHeapArrowReadData} with the given vector.
     *
     * @param vector the vector
     * @param missingValues if all, no or some values are missing
     * @param offset the offset
     * @param length the length of this data
     */
    public AbstractOnHeapArrowReadData(final T data, final ValidityBuffer validity, final int offset,
        final int length) {
        m_data = data;
        m_validity = validity;
        m_offset = offset;
        m_length = length;
    }

    @Override
    public final boolean isMissing(final int index) {
        return !m_validity.isSet(index + m_offset);
    }

    @Override
    public int length() {
        return m_length;
    }

    @Override
    public ValidityBuffer getValidityBuffer() {
        return m_validity;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Override this method to clean up additional resources or if setting the data object to <code>null</code> is not
     * enough.
     */
    @Override
    protected void closeResources() {
        m_data = null;
        m_validity = null;
    }

    /**
     * Check if the data object is sliced and throw an exception if it is.
     */
    protected void checkNotSliced() {
        if (m_offset != 0) {
            throw new IllegalStateException(
                "This data object is sliced and does not support this operation. This is an implementation error.");
        }
    }
}
