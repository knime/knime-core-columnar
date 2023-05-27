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
 *   Dec 17, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.object;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.knime.core.columnar.cache.object.AbstractCachedValueData.AbstractCachedLoadingValueReadData;
import org.knime.core.columnar.cache.object.AbstractCachedValueData.AbstractCachedValueDataFactory;
import org.knime.core.columnar.cache.object.AbstractCachedValueData.AbstractCachedValueWriteData;
import org.knime.core.columnar.cache.object.CachedData.CachedWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.VarBinaryData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Contains all classes related to caching of {@link VarBinaryData}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CachedVarBinaryData {

    private CachedVarBinaryData() {

    }

    /**
     * CachedDataFactory for {@link VarBinaryData}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class CachedVarBinaryDataFactory extends AbstractCachedValueDataFactory<Object[]> {

        CachedVarBinaryDataFactory(final ExecutorService executor, final Set<CachedWriteData> unclosedData,
            final CacheManager cacheManager, final ExecutorService serializationExecutor,
            final CountUpDownLatch serializationLatch) {
            super(executor, unclosedData, cacheManager, serializationExecutor, serializationLatch);
        }

        @Override
        public AbstractCachedValueWriteData<?, ?, ?> createCachedData(final NullableWriteData data,
            final Consumer<Object> cache) {
            return new CachedVarBinaryWriteData((VarBinaryWriteData)data, m_serializationExecutor, m_serializationLatch,
                cache);
        }

        @Override
        public NullableReadData createCachedData(final NullableReadData data, final Object[] array) {
            return new CachedVarBinaryLoadingReadData((VarBinaryReadData)data, array);
        }

        @Override
        protected Object[] createArray(final int length) {
            return new Object[length];
        }

    }

    /**
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    static final class CachedVarBinaryWriteData extends
        AbstractCachedValueWriteData<VarBinaryWriteData, VarBinaryReadData, Object> implements VarBinaryWriteData {

        private ObjectSerializer<?>[] m_serializers;

        CachedVarBinaryWriteData(final VarBinaryWriteData delegate, final ExecutorService executor,
            final CountUpDownLatch serializationLatch, final Consumer<Object> cache) {
            super(delegate, new Object[delegate.capacity()], executor, serializationLatch, cache);
            m_serializers = new ObjectSerializer<?>[delegate.capacity()];
        }

        @Override
        public void expandCache() {
            super.expandCache();
            m_serializers = Arrays.copyOf(m_serializers, super.capacity());
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
        public CachedVarBinaryReadData close(final int length) {
            onClose();
            return new CachedVarBinaryReadData(length);
        }

        @Override
        public VarBinaryReadData closeDelegate(final int length) {
            return m_delegate.close(length);
        }

        final class CachedVarBinaryReadData extends AbstractCachedValueReadData implements VarBinaryReadData {
            private VarBinaryReadData m_readData;

            CachedVarBinaryReadData(final int length) {
                super(length);
            }

            @Override
            public byte[] getBytes(final int index) {
                // early termination if we have the right kind of data cached
                if (m_data[index] instanceof byte[] bytes) {
                    return bytes;
                }

                // Otherwise we can either fetch the bytes from the underlying delegate.
                // We don't store this data though, because m_data contains the unserialized Java object
                // which we might want to access directly via getObject.
                if (m_readData != null) {
                    return m_readData.getBytes(index);
                } else {
                    // or serialize the currently stored object
                    return serializeObject(m_data[index], m_serializers[index]);
                }
            }

            @SuppressWarnings("unchecked")
            private <T> byte[] serializeObject(final T value, final ObjectSerializer<?> serializer) {
                var outStream = new ByteArrayOutputStream();
                try {
                    ((ObjectSerializer<T>)serializer).serialize(new DataOutputStream(outStream), value);
                } catch (IOException ex) {
                    throw new IllegalStateException("Error while retrieving serialized bytes from CachedVarBinary data", ex);
                }
                return outStream.toByteArray();
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
                return (T)m_data[index];
            }

            @Override
            public synchronized VarBinaryReadData closeWriteDelegate() {
                m_readData = super.closeWriteDelegate();
                m_serializers = null;
                return m_readData;
            }

        }
    }

    /**
     * Wrapper around {@link ObjectData} for in-heap caching.
     *
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    static final class CachedVarBinaryLoadingReadData
        extends AbstractCachedLoadingValueReadData<VarBinaryReadData, Object> implements VarBinaryReadData {

        CachedVarBinaryLoadingReadData(final VarBinaryReadData delegate, final Object[] data) {
            super(delegate, data);
        }

        @Override
        public byte[] getBytes(final int index) {
            if (!(m_data[index] instanceof byte[])) {
                // if not loaded yet or we loaded the object and not serialized bytes, we'll ask the delegate
                m_data[index] = m_delegate.getBytes(index);
            }
            return (byte[])m_data[index];
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            if (m_data[index] == null || (m_data[index] instanceof byte[])) {
                // if not loaded yet or we loaded the serialized bytes instead of the object, we'll ask the delegate
                m_data[index] = m_delegate.getObject(index, deserializer);
            }
            return (T)m_data[index];
        }

    }
}
