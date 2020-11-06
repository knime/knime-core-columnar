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

import java.util.concurrent.atomic.AtomicReferenceArray;

import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class HeapCachedWriteReadData<T> implements ObjectWriteData<T>, ObjectReadData<T> {

    private final ObjectWriteData<T> m_writeDelegate;

    private final AtomicReferenceArray<T> m_data;

    private ObjectReadData<T> m_readDelegate;

    private int m_length = - 1;

 // reference difference between close and serialize
    private int m_refDiff = 0;

    HeapCachedWriteReadData(final ObjectWriteData<T> delegate) {
        m_writeDelegate = delegate;
        m_data = new AtomicReferenceArray<>(delegate.capacity());
    }

    @Override
    public void setMissing(final int index) {
    }

    @Override
    public boolean isMissing(final int index) {
        return m_data.get(index) == null;
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
    public synchronized int sizeOf() {
        return m_readDelegate != null ? m_readDelegate.sizeOf() : m_writeDelegate.sizeOf();
    }

    @Override
    public void expand(final int minimumCapacity) {
        m_writeDelegate.expand(minimumCapacity);
    }

    @Override
    public void setObject(final int index, final T obj) {
        m_data.set(index, obj);
    }

    @Override
    public T getObject(final int index) {
        return m_data.get(index);
    }

    AtomicReferenceArray<T> getData() {
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

    synchronized void serialize() {
        checkClosed();
        if (!isSerialized()) {
            for (int i = 0; i < m_length; i++) {
                final T t = m_data.get(i);
                if (t != null) {
                    m_writeDelegate.setObject(i, t);
                }
            }
            m_readDelegate = m_writeDelegate.close(m_length);
            for (int i = 0; i < m_refDiff; i++) {
                m_readDelegate.retain();
            }
        }
    }

    private boolean isSerialized() {
        return m_readDelegate != null;
    }

    ObjectReadData<?> getDelegate() {
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