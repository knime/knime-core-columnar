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
package org.knime.core.columnar.badger;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.UnaryOperator;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.DataIndex;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StringData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchWritable} that intercepts {@link StringData} and {@link VarBinaryReadData} for in-heap caching of
 * objects.
 */
public final class HeapCachingBatchWritable implements BatchWritable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeapCachingBatchWritable.class);

    private final HeapCache m_heapCache;

    private final HeapCachingWriter m_writer;

    /**
     * @param writable the delegate to which to write
     * @param cache the in-heap cache for storing object data
     */
    @SuppressWarnings("resource")
    public HeapCachingBatchWritable(final BatchWritable writable, final HeapCache cache) {
        m_heapCache = cache;
        m_writer = new HeapCachingWriter(writable, m_heapCache);
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_writer.m_schema;
    }

    private static final class HeapCachingWriteBatch implements WriteBatch {

        private final WriteBatch m_delegate;

        private final NullableWriteData[] m_wrappedData;

        private HeapCachingWriteBatch(final WriteBatch delegate, final UnaryOperator<NullableWriteData[]> wrapper) {
            m_delegate = delegate;
            m_wrappedData = wrapper.apply(m_delegate.getUnsafe());
        }

        @Override
        public int numData() {
            return m_delegate.numData();
        }

        @Override
        public NullableWriteData get(final int index) {
            return m_wrappedData[index];
        }

        @Override
        public NullableWriteData[] getUnsafe() {
            return m_wrappedData;
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
            return m_delegate.sizeOf();
        }

        @Override
        public void expand(final int minimumCapacity) {
            m_delegate.expand(minimumCapacity);
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return m_delegate.usedSizeFor(numElements);
        }

        @Override
        public int capacity() {
            return m_delegate.capacity();
        }

        @Override
        public ReadBatch close(final int length) {
            final var readData = new NullableReadData[m_wrappedData.length];
            Arrays.setAll(readData, i -> m_wrappedData[i].close(length));
            return new DefaultReadBatch(readData);
        }
    }

    private final static class HeapCachingWriter implements BatchWriter {

        private final BatchWriter m_writerDelegate;

        private final ColumnarSchema m_schema;

        private final UnaryOperator<NullableWriteData[]> m_wrapWriteData;

        private final CacheReadData<NullableReadData[]> m_cacher;

        private int m_numBatchesWritten;

        private HeapCachingWriter(final BatchWritable writable, final HeapCache cache) {
            m_writerDelegate = writable.getWriter();
            m_schema = writable.getSchema();
            m_wrapWriteData = createWrapper(m_schema);
            m_cacher = createCacher(m_schema, cache);
        }

        @Override
        public WriteBatch create(final int capacity) {
            return new HeapCachingWriteBatch(m_writerDelegate.create(capacity), m_wrapWriteData);
        }

        @Override
        public void write(final ReadBatch batch) throws IOException {
            m_cacher.cache(batch.getUnsafe(), m_numBatchesWritten);
            m_writerDelegate.write(batch);
            m_numBatchesWritten++;
        }

        @Override
        public void close() throws IOException {
            m_writerDelegate.close();
        }

        @Override
        public int initialNumBytesPerElement() {
            return m_writerDelegate.initialNumBytesPerElement();
        }
    }

    private static UnaryOperator<NullableWriteData[]> createWrapper(final ColumnarSchema schema) {
        @SuppressWarnings("unchecked")
        final UnaryOperator<NullableWriteData>[] wrappers = new UnaryOperator[schema.numColumns()];
        Arrays.setAll(wrappers, i -> createWrapper(schema.getSpec(i)));
        if (Arrays.stream(wrappers).noneMatch(CachingWriteDataWrapper.class::isInstance)) {
            // if none of the wrappers are caching, we don't need to wrap anything
            return data -> data;
        } else {
            return data -> {
                var wrapped = new NullableWriteData[data.length];
                Arrays.setAll(wrapped, i -> wrappers[i].apply(data[i]));
                return wrapped;
            };
        }
    }

    private static UnaryOperator<NullableWriteData> createWrapper(final DataSpec spec) {
        if (spec instanceof StringDataSpec) {
            return CachingStringWriteData.WRAPPER;
        } else if (spec instanceof VarBinaryDataSpec) {
            return CachingVarBinaryWriteData.WRAPPER;
        } else if (spec instanceof StructDataSpec structSpec) {
            @SuppressWarnings("unchecked")
            final UnaryOperator<NullableWriteData>[] innerWrappers = new UnaryOperator[structSpec.size()];
            Arrays.setAll(innerWrappers, i -> createWrapper(structSpec.getDataSpec(i)));
            return CachingStructWriteData.wrapper(innerWrappers);
        } else {
            return d -> d;
        }
        // TODO AP-18333: Properly implement caching lists of objects
    }

    @FunctionalInterface
    private interface CacheReadData<T> {
        void cache(T data, int batchIndex);
    }

    private static CacheReadData<NullableReadData[]> createCacher(final ColumnarSchema schema,
        final HeapCache heapCache) {
        @SuppressWarnings("unchecked")
        final CacheReadData<NullableReadData>[] cachers = new CacheReadData[schema.numColumns()];
        Arrays.setAll(cachers, i -> createCacher(schema.getSpec(i), heapCache, DataIndex.createColumnIndex(i)));
        // TODO (TP): optimization: detect if all cachers are NOOPs, and return a NOOP
        return (data, batchIndex) -> {
            for (int i = 0; i < cachers.length; i++) {
                cachers[i].cache(data[i], batchIndex);
            }
        };
    }

    /**
     * @param dataIndex the index of the data (may be nested)
     */
    private static CacheReadData<NullableReadData> createCacher(final DataSpec spec, final HeapCache heapCache,
        final DataIndex dataIndex) {
        if (spec instanceof StringDataSpec) {
            return CachingStringReadData.cacher(heapCache, dataIndex);
        } else if (spec instanceof VarBinaryDataSpec) {
            return CachingVarBinaryReadData.cacher(heapCache, dataIndex);
        } else if (spec instanceof StructDataSpec structSpec) {
            @SuppressWarnings("unchecked")
            final CacheReadData<NullableReadData>[] innerCachers = new CacheReadData[structSpec.size()];
            Arrays.setAll(innerCachers, i -> createCacher(structSpec.getDataSpec(i), heapCache, dataIndex.getChild(i)));
            return CachingStructReadData.cacher(heapCache, dataIndex, innerCachers);
        } else {
            return (data, batchIndex) -> {
                // NOOP
            };
        }
        // TODO AP-18333: Properly implement caching lists of objects
    }

    private static abstract class AbstractCachingWriteData<T extends NullableWriteData, R extends NullableReadData>
        implements NullableWriteData {

        final T m_delegate;

        private AbstractCachingWriteData(final T delegate) {
            m_delegate = delegate;
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
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
        public long usedSizeFor(final int numElements) {
            return m_delegate.usedSizeFor(numElements);
        }

        @Override
        public int capacity() {
            return m_delegate.capacity();
        }

        @Override
        public final void expand(final int minimumCapacity) {
            m_delegate.expand(minimumCapacity);
            expandCache(minimumCapacity);
        }

        abstract void expandCache(int minimumCapacity);

        @Override
        public final void setMissing(final int index) {
            m_delegate.setMissing(index);
            setMissingCache(index);
        }

        abstract void setMissingCache(int index);

        @Override
        public final R close(final int length) {
            @SuppressWarnings("unchecked")
            final R readData = (R)m_delegate.close(length);
            return closeCache(readData, length);
        }

        abstract R closeCache(R delegate, int length);
    }

    private static abstract class AbstractCachingReadData<T extends NullableReadData> implements NullableReadData {

        final T m_delegate;

        private AbstractCachingReadData(final T delegate) {
            m_delegate = delegate;
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
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
        public boolean isMissing(final int index) {
            return m_delegate.isMissing(index);
        }

        @Override
        public int length() {
            return m_delegate.length();
        }
    }

    private interface CachingWriteDataWrapper extends UnaryOperator<NullableWriteData> {
    }

    private static final class CachingStringWriteData extends AbstractCachingWriteData<StringWriteData, StringReadData>
        implements StringWriteData {

        static final CachingWriteDataWrapper WRAPPER =
            delegate -> new CachingStringWriteData((StringWriteData)delegate);

        private String[] m_data;

        private CachingStringWriteData(final StringWriteData delegate) {
            super(delegate);
            m_data = new String[delegate.capacity()];
        }

        @Override
        public void setString(final int index, final String val) {
            m_delegate.setString(index, val);
            m_data[index] = val;
        }

        @Override
        public void setMissingCache(final int index) {
            m_data[index] = null;
        }

        @Override
        void expandCache(final int minimumCapacity) {
            m_data = Arrays.copyOf(m_data, m_delegate.capacity());
        }

        @Override
        StringReadData closeCache(final StringReadData delegate, final int length) {
            return new CachingStringReadData(delegate, Arrays.copyOf(m_data, length));
        }
    }

    private static final class CachingStringReadData extends AbstractCachingReadData<StringReadData>
        implements StringReadData {

        static CacheReadData<NullableReadData> cacher(final HeapCache heapCache, final DataIndex dataIndex) {
            return (data, batchIndex) -> {
                final var id = new ColumnDataUniqueId(heapCache, dataIndex, batchIndex);
                heapCache.cacheData(((CachingStringReadData)data).m_data, id);
            };
        }

        private String[] m_data;

        private CachingStringReadData(final StringReadData delegate, final String[] cachedData) {
            super(delegate);
            m_data = cachedData;
        }

        @Override
        public String getString(final int index) {
            return m_data[index];
        }

        String[] getCachedData() {
            return m_data;
        }
    }

    private static final class CachingVarBinaryWriteData
        extends AbstractCachingWriteData<VarBinaryWriteData, VarBinaryReadData>
        implements VarBinaryWriteData {

        static final CachingWriteDataWrapper WRAPPER =
            delegate -> new CachingVarBinaryWriteData((VarBinaryWriteData)delegate);

        private Object[] m_data;

        private CachingVarBinaryWriteData(final VarBinaryWriteData delegate) {
            super(delegate);
            m_data = new Object[delegate.capacity()];
        }

        @Override
        public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
            m_delegate.setObject(index, value, serializer);
            m_data[index] = value;
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            m_delegate.setBytes(index, val);
        }

        @Override
        public void setMissingCache(final int index) {
            m_data[index] = null;
        }

        @Override
        void expandCache(final int minimumCapacity) {
            m_data = Arrays.copyOf(m_data, m_delegate.capacity());
        }

        @Override
        VarBinaryReadData closeCache(final VarBinaryReadData delegate, final int length) {
            return new CachingVarBinaryReadData(delegate, Arrays.copyOf(m_data, length));
        }
    }

    private static final class CachingVarBinaryReadData extends AbstractCachingReadData<VarBinaryReadData>
        implements VarBinaryReadData {

        static CacheReadData<NullableReadData> cacher(final HeapCache heapCache, final DataIndex dataIndex) {
            return (data, batchIndex) -> {
                final var id = new ColumnDataUniqueId(heapCache, dataIndex, batchIndex);
                heapCache.cacheData(((CachingVarBinaryReadData)data).m_data, id);
            };
        }

        private Object[] m_data;

        private CachingVarBinaryReadData(final VarBinaryReadData delegate, final Object[] cachedData) {
            super(delegate);
            m_data = cachedData;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            return (T)m_data[index];
        }

        @Override
        public byte[] getBytes(final int index) {
            return m_delegate.getBytes(index);
        }

        Object[] getCachedData() {
            return m_data;
        }
    }

    private static final class CachingStructWriteData extends AbstractCachingWriteData<StructWriteData, StructReadData>
        implements StructWriteData {

        static final CachingWriteDataWrapper wrapper(final UnaryOperator<NullableWriteData>[] innerWrappers) {
            return d -> {
                var delegate = (StructWriteData)d;
                var cachingChildren = new NullableWriteData[innerWrappers.length];
                Arrays.setAll(cachingChildren, i -> innerWrappers[i].apply(delegate.getWriteDataAt(i)));
                return new CachingStructWriteData(delegate, cachingChildren);
            };
        }

        private final NullableWriteData[] m_cachingChildren;

        private CachingStructWriteData(final StructWriteData delegate, final NullableWriteData[] cachingChildren) {
            super(delegate);
            m_cachingChildren = cachingChildren;
        }

        @Override
        public void setMissingCache(final int index) {
            for (var child : m_cachingChildren) {
                ((AbstractCachingWriteData<?, ?>)child).setMissingCache(index);
            }
        }

        @Override
        void expandCache(final int minimumCapacity) {
            for (var child : m_cachingChildren) {
                ((AbstractCachingWriteData<?, ?>)child).expandCache(minimumCapacity);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <C extends NullableWriteData> C getWriteDataAt(final int index) {
            return (C)m_cachingChildren[index];
        }

        @Override
        StructReadData closeCache(final StructReadData delegate, final int length) {
            final var readChildren = new NullableReadData[m_cachingChildren.length];
            for (int i = 0; i < m_cachingChildren.length; ++i) {
                readChildren[i] = ((AbstractCachingWriteData<?, ?>)m_cachingChildren[i])
                    .closeCache(delegate.getReadDataAt(i), length);
            }
            return new CachingStructReadData(delegate, readChildren);
        }
    }

    private static final class CachingStructReadData extends AbstractCachingReadData<StructReadData>
        implements StructReadData {

        static CacheReadData<NullableReadData> cacher(final HeapCache heapCache, final DataIndex dataIndex,
            final CacheReadData<NullableReadData>[] innerCachers) {
            return (data, batchIndex) -> {
                for (int i = 0; i < innerCachers.length; ++i) {
                    innerCachers[i].cache(((StructReadData)data).getReadDataAt(i), batchIndex);
                }
            };
        }

        private final NullableReadData[] m_cachingChildren;

        private CachingStructReadData(final StructReadData delegate, final NullableReadData[] cachingChildren) {
            super(delegate);
            m_cachingChildren = cachingChildren;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <C extends NullableReadData> C getReadDataAt(final int index) {
            return (C)m_cachingChildren[index];
        }
    }
}
