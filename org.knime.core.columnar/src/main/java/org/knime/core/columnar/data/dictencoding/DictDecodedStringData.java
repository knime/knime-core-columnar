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

import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedData.AbstractDictDecodedReadData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedData.AbstractDictDecodedWriteData;
import org.knime.core.columnar.data.dictencoding.DictElementCache.ColumnDictElementCache;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringWriteData;

/**
 * Provide {@link StringWriteData} and {@link StringReadData} access to underlying dictionary encoded data by allowing
 * to use a table-wide cache.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DictDecodedStringData {

    private DictDecodedStringData() {
    }

    /**
     * {@link DictDecodedStringWriteData} provides table-wide caching and {@link StringWriteData} access to a wrapped
     * {@link DictEncodedStringWriteData}.
     *
     * @param <K> key type, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictDecodedStringWriteData<K> extends AbstractDictDecodedWriteData<K, DictEncodedStringWriteData<K>>
        implements StringWriteData {

        /**
         * Create a {@link DictDecodedStringWriteData} wrapping a {@link DictEncodedStringWriteData} provided by a
         * back-end.
         *
         * @param delegate The delegate {@link DictEncodedStringWriteData}
         * @param cache The table-wide {@link ColumnDictElementCache} for dictionary entries, also used to generate
         *            global dictionary keys
         */
        public DictDecodedStringWriteData(final DictEncodedStringWriteData<K> delegate,
            final ColumnDictElementCache<K> cache) {
            super(delegate, cache);
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
            m_delegate.setString(index, val);
        }

        @SuppressWarnings("unchecked")
        @Override
        public StringReadData close(final int length) {
            return new DictDecodedStringReadData<K>((DictEncodedStringReadData<K>)m_delegate.close(length), m_cache);
        }
    }

    /**
     * {@link DictDecodedStringReadData} provides table-wide caching and {@link StringReadData} access to a wrapped
     * {@link DictEncodedStringReadData}.
     *
     * @param <K> key type, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictDecodedStringReadData<K> extends AbstractDictDecodedReadData<K, DictEncodedStringReadData<K>>
        implements StringReadData {
        /**
         * Create a {@link DictDecodedStringReadData} wrapping a {@link DictEncodedStringReadData} provided by a
         * back-end.
         *
         * @param delegate The delegate {@link DictEncodedStringReadData}
         * @param cache The table-wide {@link ColumnDictElementCache} for dictionary entries
         */
        public DictDecodedStringReadData(final DictEncodedStringReadData<K> delegate, final ColumnDictElementCache<K> cache) {
            super(delegate, cache);
        }

        @Override
        public String getString(final int index) {
            // TODO: for global dict caching:
            //            int dictKey = m_delegate.getDictEntryKey(index);
            //            return m_cache.computeIfAbsent(dictKey, d -> m_delegate.getString(index));

            return m_delegate.getString(index);
        }
    }

}
