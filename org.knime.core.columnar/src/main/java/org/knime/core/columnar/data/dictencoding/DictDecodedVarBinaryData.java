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

import java.util.HashMap;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedObjectData.AbstractDictDecodedObjectReadData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedObjectData.AbstractDictDecodedObjectWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedVarBinaryData.DictEncodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedVarBinaryData.DictEncodedVarBinaryWriteData;
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
     * {@link WriteData} that stores Strings using a dictionary encoding
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictDecodedVarBinaryWriteData extends AbstractDictDecodedObjectWriteData<Object, DictEncodedVarBinaryWriteData>
        implements VarBinaryWriteData {

        final HashMap<Integer, ObjectSerializer<Object>> m_serializers = new HashMap<>();

        /**
         * Create a {@link DictDecodedVarBinaryWriteData} wrapping a {@link DictDecodedVarBinaryWriteData} provided by a
         * backend.
         *
         * @param delegate The delegate {@link DictEncodedVarBinaryWriteData}
         */
        public DictDecodedVarBinaryWriteData(final DictEncodedVarBinaryWriteData delegate) {
            super(delegate);
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            setDictEncodedObject(index, val, (output, v) -> {
                output.writeInt(v.length);
                output.write(v);
            });
        }

        private <T> void setDictEncodedObject(final int index, final T obj, final ObjectSerializer<T> serializer) {
            @SuppressWarnings("unchecked")
            final int dictIndex = m_dict.computeIfAbsent(obj, o -> {
                m_serializers.put(m_nextDictEntry, (ObjectSerializer<Object>)serializer);
                return m_nextDictEntry++;
            });
            m_delegate.setReference(index, dictIndex);
        }

        @Override
        public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
            setDictEncodedObject(index, value, serializer);
        }

        @Override
        public VarBinaryReadData close(final int length) {
            // write all dict entries:
            m_dict.forEach((k, v) -> m_delegate.setDictEntry(v, k, m_serializers.get(v)));

            // now we can close
            return new DictDecodedVarBinaryReadData(m_delegate.close(length));
        }
    }

    /**
     * {@link ReadData} holding String elements.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static class DictDecodedVarBinaryReadData extends AbstractDictDecodedObjectReadData<Object, DictEncodedVarBinaryReadData>
        implements VarBinaryReadData {

        /**
         * Create a {@link DictDecodedVarBinaryReadData} wrapping a {@link DictDecodedVarBinaryReadData} provided by a
         * backend.
         *
         * @param delegate The delegate {@link DictEncodedVarBinaryReadData}
         */
        public DictDecodedVarBinaryReadData(final DictEncodedVarBinaryReadData delegate) {
            super(delegate);
        }

        @Override
        public byte[] getBytes(final int index) {
            return getDictEncodedObject(index, input -> {
                final int length = input.readInt();
                byte[] buf = new byte[length];
                input.readFully(buf, 0, length);
                return buf;
            });
        }

        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            return getDictEncodedObject(index, deserializer);
        }

        @SuppressWarnings("unchecked")
        private <T> T getDictEncodedObject(final int index, final ObjectDeserializer<T> deserializer) {
            final int dictIndex = m_delegate.getReference(index);
            return (T)m_dict.computeIfAbsent(dictIndex, i -> m_delegate.getDictEntry(i, deserializer));
        }
    }

}
