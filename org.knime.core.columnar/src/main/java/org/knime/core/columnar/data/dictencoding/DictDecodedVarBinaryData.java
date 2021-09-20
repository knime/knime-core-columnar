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

import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedData.AbstractDictDecodedReadData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedData.AbstractDictDecodedWriteData;
import org.knime.core.columnar.data.dictencoding.DictElementCache.ColumnDictElementCache;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Class holding {@link VarBinaryWriteData}, {@link VarBinaryReadData} holding VarBinary objects
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DictDecodedVarBinaryData {

    private DictDecodedVarBinaryData() {
    }

    /**
     * {@link DictDecodedVarBinaryWriteData} provides table-wide caching and {@link VarBinaryWriteData} access to a
     * wrapped {@link DictEncodedVarBinaryWriteData}.
     *
     * @param <K> key type, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictDecodedVarBinaryWriteData<K>
        extends AbstractDictDecodedWriteData<K, DictEncodedVarBinaryWriteData<K>> implements VarBinaryWriteData {

        /**
         * Create a {@link DictDecodedVarBinaryWriteData} wrapping a {@link DictDecodedVarBinaryWriteData} provided by a
         * back-end.
         *
         * @param delegate The delegate {@link DictEncodedVarBinaryWriteData}
         * @param cache The table-wide {@link ColumnDictElementCache} for dictionary entries, also used to generate
         *            global dictionary keys
         */
        public DictDecodedVarBinaryWriteData(final DictEncodedVarBinaryWriteData<K> delegate,
            final ColumnDictElementCache<K> cache) {
            super(delegate, cache);
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            m_delegate.setBytes(index, val);
        }

        @Override
        public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
            m_delegate.setObject(index, value, serializer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public VarBinaryReadData close(final int length) {
            return new DictDecodedVarBinaryReadData<K>((DictEncodedVarBinaryReadData<K>)m_delegate.close(length), m_cache);
        }
    }

    /**
     * {@link DictDecodedVarBinaryReadData} provides table-wide caching and {@link VarBinaryReadData} access to a
     * wrapped {@link DictEncodedVarBinaryReadData}.
     *
     * @param <K> key type, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictDecodedVarBinaryReadData<K> extends AbstractDictDecodedReadData<K, DictEncodedVarBinaryReadData<K>>
        implements VarBinaryReadData {

        /**
         * Create a {@link DictDecodedVarBinaryReadData} wrapping a {@link DictDecodedVarBinaryReadData} provided by a
         * back-end.
         *
         * @param delegate The delegate {@link DictEncodedVarBinaryReadData}
         * @param cache The table-wide {@link ColumnDictElementCache} for dictionary entries
         */
        public DictDecodedVarBinaryReadData(final DictEncodedVarBinaryReadData<K> delegate,
            final ColumnDictElementCache<K> cache) {
            super(delegate, cache);
        }

        @Override
        public byte[] getBytes(final int index) {
            // TODO: for global dict caching:
            //          int dictKey = m_delegate.getDictEntryKey(index);
            //          return m_cache.computeIfAbsent(dictKey, d -> m_delegate.getBytes(index));

            return m_delegate.getBytes(index);
        }

        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            // TODO: for global dict caching:
            //          int dictKey = m_delegate.getDictEntryKey(index);
            //          return m_cache.computeIfAbsent(dictKey, d -> m_delegate.getObject(index));

            return m_delegate.getObject(index, deserializer);
        }
    }
}
