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
 */
package org.knime.core.columnar.cache.object;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class CachedVarBinaryWriteData extends CachedWriteData<VarBinaryWriteData, VarBinaryReadData, Object>
    implements VarBinaryWriteData {

    final class CachedVarBinaryReadData extends CachedReadData implements VarBinaryReadData {

        CachedVarBinaryReadData(final int length) {
            super(length);
        }

        @Override
        public byte[] getBytes(final int index) {
            return (byte[])m_data[index];
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            return (T)m_data[index];
        }

        @Override
        synchronized VarBinaryReadData close() {
            VarBinaryReadData out = super.close();
            m_serializers = null;
            return out;
        }

    }

    private ObjectSerializer<?>[] m_serializers;

    CachedVarBinaryWriteData(final VarBinaryWriteData delegate, final ExecutorService executor) {
        super(delegate, new Object[delegate.capacity()], executor);
        m_serializers = new ObjectSerializer<?>[delegate.capacity()];
    }

    @Override
    public void expand(final int minimumCapacity) {
        super.expand(minimumCapacity);
        m_serializers = Arrays.copyOf(m_serializers, m_delegate.capacity());
    }

    @Override
    public void setBytes(final int index, final byte[] val) {
        m_data[index] = val;
        onSet(index);
    }

    @Override
    public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
        m_data[index] = value;
        m_serializers[index] = serializer;
        onSet(index);
    }

    @Override
    void serializeAt(final int index) {
        final ObjectSerializer<?> serializer = m_serializers[index];
        if (serializer != null) {
            serializeObject(index, m_data[index], serializer);
        } else {
            m_delegate.setBytes(index, (byte[])m_data[index]);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void serializeObject(final int index, final Object value, final ObjectSerializer<?> serializer) {
        m_delegate.setObject(index, (T)value, (ObjectSerializer<T>)serializer);
    }

    @Override
    public VarBinaryReadData close(final int length) {
        onClose();
        return new CachedVarBinaryReadData(length);
    }

    @Override
    VarBinaryReadData closeDelegate(final int length) {
        return m_delegate.close(length);
    }

}
