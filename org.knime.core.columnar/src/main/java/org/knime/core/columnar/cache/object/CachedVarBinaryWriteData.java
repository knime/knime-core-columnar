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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class CachedVarBinaryWriteData implements VarBinaryWriteData {

    final class CachedVarBinaryReadData implements VarBinaryReadData {

        private VarBinaryReadData m_readDelegate;

        private final int m_length;

        private int m_numRetainsPriorSerialize = 0;

        CachedVarBinaryReadData(final int length) {
            m_length = length;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_data[index] == null;
        }

        @Override
        public int length() {
            return m_length;
        }

        @Override
        public synchronized void retain() {
            if (m_readDelegate != null) {
                m_readDelegate.retain();
            } else {
                m_numRetainsPriorSerialize++;
            }
        }

        @Override
        public synchronized void release() {
            if (m_readDelegate != null) {
                m_readDelegate.release();
            } else {
                if (m_numRetainsPriorSerialize == 0) {
                    // as per contract, the HeapCachedWriteData's ref count was at 1 when it was closed
                    // therefore, the reference count would drop to 0 here and we have to release the HeapCachedWriteData
                    // this means that a subsequent call to serialize should and will fail
                    CachedVarBinaryWriteData.this.release();
                } else {
                    m_numRetainsPriorSerialize--;
                }
            }
        }

        @Override
        public synchronized long sizeOf() {
            return m_readDelegate != null ? m_readDelegate.sizeOf() : CachedVarBinaryWriteData.this.sizeOf();
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

        Object[] getData() {
            return m_data;
        }

        synchronized VarBinaryReadData serialize() {
            CachedVarBinaryWriteData.this.serialize();

            m_readDelegate = CachedVarBinaryWriteData.this.getDelegate().close(m_length);
            if (m_numRetainsPriorSerialize > 0) {
                for (int i = 0; i < m_numRetainsPriorSerialize; i++) {
                    m_readDelegate.retain();
                }
            }

            return m_readDelegate;
        }

    }

    private final VarBinaryWriteData m_delegate;

    // We should not need a thread-safe data structure (i.e., an AtomicReferenceArray) here.
    // Reasoning: In Java, there is a happens-before relation between one thread starting another.
    // Here, the "writer thread", i.e., the thread that creates the data and calls setObject() should be the same as the
    // one that starts the serialization thread in HeapCachedColumnStore#writeInternal(ReadBatch).
    private Object[] m_data;

    private Queue<Runnable> m_serialization = new ConcurrentLinkedQueue<>();

    CachedVarBinaryWriteData(final VarBinaryWriteData delegate) {
        m_delegate = delegate;
        m_data = new Object[delegate.capacity()];
    }

    @Override
    public void setMissing(final int index) {
        m_data[index] = null;
    }

    @Override
    public int capacity() {
        return m_delegate.capacity();
    }

    @Override
    public void retain() {
        m_delegate.retain();
    }

    @Override
    public void release() {
        m_delegate.release();
    }

    @Override
    public long sizeOf() {
        serialize();
        return m_delegate.sizeOf();
    }

    @Override
    public void expand(final int minimumCapacity) {
        m_delegate.expand(minimumCapacity);
        m_data = Arrays.copyOf(m_data, m_delegate.capacity());
    }

    @Override
    public void setBytes(final int index, final byte[] val) {
        m_data[index] = val;
        m_serialization.add(() -> m_delegate.setBytes(index, val));
    }

    @Override
    public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
        m_data[index] = value;
        m_serialization.add(() -> m_delegate.setObject(index, value, serializer));
    }

    @Override
    public VarBinaryReadData close(final int length) {
        return new CachedVarBinaryReadData(length);
    }

    void serialize() {
        Runnable r;
        while ((r = m_serialization.poll()) != null) {
            r.run();
        }
    }

    VarBinaryWriteData getDelegate() {
        return m_delegate;
    }

}