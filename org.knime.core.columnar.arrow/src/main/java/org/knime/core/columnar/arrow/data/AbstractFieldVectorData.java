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
package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;

/**
 * Abstract implementation of {@link ColumnReadData} and {@link ColumnWriteData} using Arrow. Make sure to call
 * <code>#closeWithLength(int)</code> on {@link ColumnWriteData#close(int)}.
 *
 * @param <F> Type of the field vector holding the data.
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractFieldVectorData<F extends FieldVector> extends AbstractReferenceData
    implements ArrowReadData, ArrowWriteData {

    /** The Arrow {@link FieldVector} holding the data */
    protected final F m_vector;

    protected int m_offset;

    protected int m_length;

    /**
     * Create by wrapping the given vector. Can be an empty vector or a vector already containing data.
     *
     * @param vector the vector holding the data
     */
    AbstractFieldVectorData(final F vector) {
        m_vector = vector;
        m_offset = 0;
        m_length = vector.getValueCount();
    }

    @Override
    public boolean isMissing(final int index) {
        return m_vector.isNull(index + m_offset);
    }

    @Override
    public void setMissing(final int index) {
        @SuppressWarnings("resource") // Validity buffer handled by vector
        final ArrowBuf validityBuffer = m_vector.getValidityBuffer();
        BitVectorHelper.unsetBit(validityBuffer, m_offset + index);
    }

    @Override
    public int capacity() {
        return m_vector.getValueCapacity();
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

    @Override
    public void expand(final int minimumCapacity) {
        while (m_vector.getValueCapacity() < minimumCapacity) {
            m_vector.reAlloc();
        }
    }

    @Override
    public void slice(final int start, final int length) {
        m_offset = start;
        m_length = length;
    }

    /**
     * Call on {@link ColumnWriteData#close(int)} to set the length of this data and the value count of the vector.
     *
     * @param length the amount of values in the vector
     */
    protected void closeWithLength(final int length) {
        m_offset = 0;
        m_length = length;
        m_vector.setValueCount(length);
    }

    /**
     * An abstract implementation of {@link ArrowColumnDataFactory} for data extending AbstractFieldVectorData and
     * having no dictionaries.
     */
    protected abstract static class AbstractFieldVectorDataFactory implements ArrowColumnDataFactory {

        @Override
        public FieldVector getVector(final ColumnReadData data) {
            return ((AbstractFieldVectorData<?>)data).m_vector;
        }

        @Override
        public DictionaryProvider getDictionaries(final ColumnReadData data) {
            return null;
        }
    }
}
