package org.knime.core.columnar.cache.heap;

import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;

final class HeapCachedWriteData<T> implements ObjectWriteData<T> {

    private final ObjectWriteData<T> m_delegate;

    private final Object[] m_cache;

    //    private final Cache<Integer, T> m_cache;

    HeapCachedWriteData(final ObjectWriteData<T> delegate) {
        m_delegate = delegate;
        // TODO weak cache
        //        m_cache = CacheBuilder.newBuilder().maximumSize(Integer.MAX_VALUE).build();
        m_cache = new Object[delegate.capacity()];
    }

    @Override
    public void setMissing(final int index) {
        m_delegate.setMissing(index);
    }

    @Override
    public int capacity() {
        return m_delegate.capacity();
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

    @Override
    public void setObject(final int index, final T obj) {
        // TODO block adding when heap-memory has reached limit?
        m_cache[index] = obj;

        // TODO async writing
        m_delegate.setObject(index, obj);
    }

    @Override
    public ObjectReadData<T> close(final int length) {
        return new HeapCachedReadData<>(m_delegate.close(length), m_cache);
    }

}