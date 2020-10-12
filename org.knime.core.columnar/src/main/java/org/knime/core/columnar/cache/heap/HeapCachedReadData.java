package org.knime.core.columnar.cache.heap;

import org.knime.core.columnar.data.ObjectData.ObjectReadData;

final class HeapCachedReadData<T> implements ObjectReadData<T> {

    private final ObjectReadData<T> m_delegate;

    private final Object[] m_cache;

    public HeapCachedReadData(final ObjectReadData<T> delegate) {
        this(delegate, new Object[delegate.length()]);
        // TODO we need to try to get access back to the central cache of this ObjectReadData (scenario: two tables read from same store):
    }

    public HeapCachedReadData(final ObjectReadData<T> delegate, final Object[] cache) {
        m_delegate = delegate;
        m_cache = cache;
    }

    @Override
    public boolean isMissing(final int index) {
        return m_delegate.isMissing(index);
    }

    @Override
    public int length() {
        return m_delegate.length();
    }

    @Override
    public void release() {
        m_delegate.release();
    }

    @Override
    public void retain() {
        m_delegate.retain();
    }

    @Override
    public int sizeOf() {
        return m_delegate.sizeOf();
    }

    final ObjectReadData<T> getDelegate() {
        return m_delegate;
    }

    @Override
    public T getObject(final int index) {
        if (m_cache[index] == null) {
            m_cache[index] = m_delegate.getObject(index);
        }
        return (T)m_cache[index];
    }

}