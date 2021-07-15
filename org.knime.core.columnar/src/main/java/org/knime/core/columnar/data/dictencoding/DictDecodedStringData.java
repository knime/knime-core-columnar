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
 *   Nov 30, 2020 (benjamin): created
 */
package org.knime.core.columnar.data.dictencoding;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedObjectData.AbstractDictDecodedObjectReadData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedObjectData.AbstractDictDecodedObjectWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedStringData.DictEncodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedStringData.DictEncodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedVarBinaryData.DictEncodedVarBinaryReadData;

/**
 * Provide {@link StringWriteData} and {@link StringReadData} access to underlying dictionary encoded data
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DictDecodedStringData {

    private DictDecodedStringData() {
    }

    /**
     * {@link DictDecodedStringWriteData} provides {@link StringWriteData} access to underlying {@link DictEncodedStringWriteData}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictDecodedStringWriteData extends AbstractDictDecodedObjectWriteData<String> implements StringWriteData {

        /**
         * Create a {@link DictDecodedStringWriteData} wrapping a {@link DictEncodedStringWriteData} provided by a backend.
         * @param delegate The delegate {@link DictEncodedStringWriteData}
         */
        public DictDecodedStringWriteData(final DictEncodedStringWriteData delegate) {
            super(delegate);
        }

        /**
         * Assigns a String value to the element at the given index. The contract is that values are only ever set for
         * ascending indices. It is the responsibility of the client calling this method to make sure that the provided
         * index is non-negative and smaller than the capacity of this {@link WriteData}.
         *
         * @param index the index at which to set the String value
         * @param val the String value to set
         */
        @Override
        public void setString(final int index, final String val) {
            setDictEncodedObject(index, val);
        }

        private void setDictEncodedObject(final int index, final String val) {
            final var dictIndex = m_dict.computeIfAbsent(val, o -> m_nextDictEntry++);
            m_delegate.setReference(index, dictIndex);
        }

        @Override
        public StringReadData close(final int length) {
            // write all dict entries:
            m_dict.entrySet().stream()
                .sorted((a, b) -> Integer.compare(a.getValue(), b.getValue()))
                .forEach(e -> ((DictEncodedStringWriteData)m_delegate).setDictEntry(e.getValue(), e.getKey()));

            // now we can close
            return new DictDecodedStringReadData((DictEncodedStringReadData)m_delegate.close(length));
        }
    }

    /**
     * {@link DictDecodedStringReadData} provides {@link StringReadData} access to underlying {@link DictEncodedVarBinaryReadData}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictDecodedStringReadData extends AbstractDictDecodedObjectReadData<String> implements StringReadData {
        /**
         * Create a {@link DictDecodedStringReadData} wrapping a {@link DictEncodedStringReadData} provided by a backend.
         * @param delegate The delegate {@link DictEncodedStringReadData}
         */
        public DictDecodedStringReadData(final DictEncodedStringReadData delegate) {
            super(delegate);
        }

        /**
         * Obtains the String value at the given index. It is the responsibility of the client calling this method to
         * make sure that the provided index is non-negative and smaller than the length of this {@link ReadData}.
         *
         * @param index the index at which to obtain the String element
         * @return the String element at the given index
         */
        @Override
        public String getString(final int index) {
            return getDictEncodedObject(index);
        }

        private String getDictEncodedObject(final int index) {
            final var dictIndex = m_delegate.getReference(index);
            return m_dict.computeIfAbsent(dictIndex, ((DictEncodedStringReadData)m_delegate)::getDictEntry);
        }
    }

}
