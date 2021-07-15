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

import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.VectorDefinitionSetter;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedWriteData;

/**
 * Arrow implementation of dictionary encoded data, where dictionary elements can be accessed additional to the
 * referenced indices into the dictionary.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class AbstractArrowDictEncodedData {

    private AbstractArrowDictEncodedData() {
    }

    static final int INIT_DICT_BYTE_SIZE = 1024;

    static final int INIT_DICT_NUM_ENTRIES = 20;

    /**
     * Arrow implementation of {@link DictEncodedWriteData}.
     * @param <T> The type of the Arrow vector storing the dictionary entries
     */
    public abstract static class ArrowDictEncodedWriteData<T extends BaseValueVector & VariableWidthVector & FieldVector & VectorDefinitionSetter>
        extends AbstractArrowWriteData<IntVector> implements DictEncodedWriteData {

        /**
         * The dictionary vector
         */
        protected T m_dictVector;

        /**
         * The largest dictionary entry index that was set. Needed to set the
         * value count of the m_dictVector on close().
         */
        protected int m_largestDictionaryIndex = 0;

        /**
         * Create an {@link ArrowDictEncodedWriteData} object with a vector of references
         * (an integer for each value referencing into the dictionary), and a vector containing
         * the dictionary entries, which must have ascending consecutive indices.
         *
         * @param vector The vector containing a dictionary element index per value
         * @param dictVector The vector containing the dictionary entries
         */
        protected ArrowDictEncodedWriteData(final IntVector vector, final T dictVector) {
            super(vector);
            m_dictVector = dictVector;
        }

        /**
         * Create an {@link ArrowDictEncodedWriteData} object with a vector of references
         * (an integer for each value referencing into the dictionary), and a vector containing
         * the dictionary entries, which must have ascending consecutive indices.
         *
         * @param vector The vector containing a dictionary element index per value
         * @param offset The offset into the vector
         * @param dictVector The vector containing the dictionary entries
         */
        protected ArrowDictEncodedWriteData(final IntVector vector, final int offset, final T dictVector) {
            super(vector, offset);
            m_dictVector = dictVector;
        }

        @Override
        public void setReference(final int dataIndex, final int dictionaryIndex) {
            m_vector.set(m_offset + dataIndex, dictionaryIndex);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector) + ArrowSizeUtils.sizeOfFixedWidth(m_dictVector);
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_dictVector.setValueCount(m_largestDictionaryIndex + 1);
            m_dictVector.close();
        }

        @Override
        public String toString() {
            return super.toString() + " -> " + m_dictVector.toString();
        }
    }

    /**
     * Arrow implementation of {@link DictEncodedReadData}.
     * @param <T> The type of the Arrow vector storing the dictionary entries
     */
    public abstract static class ArrowDictEncodedReadData<T extends BaseValueVector & VariableWidthVector & FieldVector & VectorDefinitionSetter>
        extends AbstractArrowReadData<IntVector> implements DictEncodedReadData {

        /**
         * The dictionary vector
         */
        protected final T m_dictVector;

        /**
         * Create an {@link ArrowDictEncodedReadData} object with a vector of references
         * (an integer for each value referencing into the dictionary), and a vector containing
         * the dictionary entries, which must have ascending consecutive indices.
         *
         * @param vector The vector containing a dictionary element index per value
         * @param missingValues The missing values
         * @param dictVector The vector containing the dictionary entries
         */
        protected ArrowDictEncodedReadData(final IntVector vector, final MissingValues missingValues,
            final T dictVector) {
            super(vector, missingValues);
            m_dictVector = dictVector;
        }

        /**
         * Create an {@link ArrowDictEncodedReadData} object with a vector of references
         * (an integer for each value referencing into the dictionary), and a vector containing
         * the dictionary entries, which must have ascending consecutive indices.
         *
         * @param vector The vector containing a dictionary element index per value
         * @param missingValues The missing values
         * @param offset The offset into the vector
         * @param length Length of the vector (not the dictionary)
         * @param dictVector The vector containing the dictionary entries
         */
        protected ArrowDictEncodedReadData(final IntVector vector, final MissingValues missingValues,
            final int offset, final int length, final T dictVector) {
            super(vector, missingValues, offset, length);
            m_dictVector = dictVector;
        }

        T getDictionary() {
            return m_dictVector;
        }

        @Override
        public int getReference(final int dataIndex) {
            return m_vector.get(m_offset + dataIndex);
        }


        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector) + ArrowSizeUtils.sizeOfFixedWidth(m_dictVector);
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_dictVector.close();
        }

        @Override
        public String toString() {
            return super.toString() + " -> " + m_dictVector.toString();
        }
    }
}
