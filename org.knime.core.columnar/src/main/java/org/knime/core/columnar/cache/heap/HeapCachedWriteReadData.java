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
package org.knime.core.columnar.cache.heap;

import java.util.Arrays;

import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
//TODO for AP-15619: split into two classes to reduce statefulness
final class HeapCachedWriteReadData<T> implements ObjectWriteData<T>, ObjectReadData<T> {

    private final ObjectWriteData<T> m_writeDelegate;

    // We should not need a thread-safe data structure (i.e., an AtomicReferenceArray) here.
    // Reasoning: In Java, there is a happens-before relation between one thread starting another.
    // Here, the "writer thread", i.e., the thread that creates the data and calls setObject() should be the same as the
    // one that starts the serialization thread in HeapCachedColumnStore#writeInternal(ReadBatch).
    private Object[] m_data;

    private ObjectReadData<T> m_readDelegate;

    private int m_length = -1;

    private int m_serializeFromIndex = 0;

    // reference difference between close and serialize
    private int m_refDiff = 0;

    HeapCachedWriteReadData(final ObjectWriteData<T> delegate) {
        m_writeDelegate = delegate;
        m_data = new Object[delegate.capacity()];
    }

    @Override
    public void setMissing(final int index) {
        m_data[index] = null;
    }

    @Override
    public boolean isMissing(final int index) {
        return m_data[index] == null;
    }

    @Override
    public int capacity() {
        return m_writeDelegate.capacity();
    }

    @Override
    public int length() {
        checkClosed();
        return m_length;
    }

    @Override
    public synchronized void retain() {
        if (!isClosed()) {
            m_writeDelegate.retain();
        } else if (isSerialized()) {
            m_readDelegate.retain();
        } else {
            m_refDiff++;
        }
    }

    @Override
    public synchronized void release() {
        if (!isClosed()) {
            m_writeDelegate.release();
        } else if (isSerialized()) {
            m_readDelegate.release();
        } else {
            m_refDiff--;
        }
    }

    @Override
    public synchronized long sizeOf() {
    	if (isSerialized()) {
    		return m_readDelegate.sizeOf();
    	}
    	// Instead of flushing to the capacity, we could also remember the largest set index and only flush until there.
    	// But that would cost us an additional operation per value, even if sizeof is never called.
    	// So we are probably better of this way.
    	serialize(capacity());
    	return m_writeDelegate.sizeOf();
    }

    @Override
    public void expand(final int minimumCapacity) {
        m_writeDelegate.expand(minimumCapacity);
        m_data = Arrays.copyOf(m_data, m_writeDelegate.capacity());
    }

    @Override
    public void setObject(final int index, final T obj) {
        m_data[index] = obj;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T getObject(final int index) {
        return (T)m_data[index];
    }

    Object[] getData() {
        return m_data;
    }

    @Override
    public synchronized ObjectReadData<T> close(final int length) {
        m_length = length;
        return this;
    }

    private boolean isClosed() {
        return m_length != -1;
    }

    private void serialize(final int length) {
        for(int i = m_serializeFromIndex; i < length; i++) {
            final T t = getObject(i);
            if (t != null) {
                m_writeDelegate.setObject(i, t);
                // The value that has been serialized last will have to be serialized again in the next invocation of
                // serialize, since it might have been updated or set to missing in the meantime.
                m_serializeFromIndex = i;
            }
        }
    }

    synchronized void serialize() {
        checkClosed();
        if (!isSerialized()) {
            serialize(m_length);
            m_readDelegate = m_writeDelegate.close(m_length);
            for (int i = 0; i < m_refDiff; i++) {
                m_readDelegate.retain();
            }
        }
    }

    private boolean isSerialized() {
        return m_readDelegate != null;
    }

    ObjectReadData<T> getDelegate() {
        if (!isSerialized()) {
            throw new IllegalStateException("Data has not been serialized.");
        }
        return m_readDelegate;
    }

    private void checkClosed() {
        if (!isClosed()) {
            throw new IllegalStateException("Data has not been closed.");
        }
    }
}
