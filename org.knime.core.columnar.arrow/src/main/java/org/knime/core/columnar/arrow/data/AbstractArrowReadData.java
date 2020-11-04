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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * Abstract implementation of {@link ArrowReadData}. Holds a {@link FieldVector} of type F in {@link #m_vector} and an
 * offset ({@link #m_offset}) and length ({@link #m_length}) for the indices to use in this vector.
 *
 * Overwrite {@link #closeResources()} to close additional resources (make sure to call the super method).
 *
 * @param <F> the type of the {@link FieldVector}
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
abstract class AbstractArrowReadData<F extends FieldVector> extends AbstractReferencedData implements ArrowReadData {

    /** The vector. Use {@link #m_offset} when accessing vector data. */
    protected final F m_vector;

    /** An offset describing where the values of this object start in the vector. */
    protected final int m_offset;

    /** The length of the data */
    protected final int m_length;

    /**
     * Create an abstract {@link ArrowReadData} with the given vector, an offset of 0 and the length of the vector.
     *
     * @param vector the vector
     */
    public AbstractArrowReadData(final F vector) {
        m_vector = vector;
        m_offset = 0;
        m_length = vector.getValueCount();
    }

    /**
     * Create an abstract {@link ArrowReadData} with the given vector.
     *
     * @param vector the vector
     * @param offset the offset
     * @param length the length of this data
     */
    public AbstractArrowReadData(final F vector, final int offset, final int length) {
        m_vector = vector;
        m_offset = offset;
        m_length = length;
    }

    @Override
    public boolean isMissing(final int index) {
        return m_vector.isNull(m_offset + index);
    }

    @Override
    public int length() {
        return m_length;
    }

    @Override
    protected void closeResources() {
        m_vector.close();
    }

    @Override
    public String toString() {
        return ValueVectorUtility.getToString(m_vector, m_offset, m_offset + m_length);
    }
}
