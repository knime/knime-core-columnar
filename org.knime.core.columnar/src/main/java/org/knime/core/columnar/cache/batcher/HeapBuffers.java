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
 *   Oct 22, 2020 (dietzc): created
 */
package org.knime.core.columnar.cache.batcher;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.ByteAccess.ByteWriteAccess;
import org.knime.core.table.access.DelegatingReadAccesses;
import org.knime.core.table.access.DelegatingReadAccesses.DelegatingReadAccess;
import org.knime.core.table.access.DelegatingWriteAccesses;
import org.knime.core.table.access.DelegatingWriteAccesses.DelegatingWriteAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleWriteAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.FloatAccess.FloatWriteAccess;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.StructAccess.StructWriteAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryWriteAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;
import org.knime.core.table.schema.VoidDataSpec;

/**
 * A collection of buffered access implementations that can be retrieved by mapping from a given {@link DataSpec}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class HeapBuffers {

    private HeapBuffers() {
    }

    /**
     * Creates a heap buffer for the given spec with the given capacity.
     *
     * The returned buffers are NOT thread-safe and require external synchronization by the client.
     *
     * @param spec for which a buffer is required
     * @param capacity the number of elements the buffer can hold
     * @return a {@link HeapBuffer} for the provided {@link DataSpec}
     */
    public static HeapBuffer createHeapBuffer(final DataSpec spec, final int capacity) {
        return spec.accept(new DataSpecToHeapBufferMapper(capacity));
    }

    /**
     * A buffer in heap memory that allows to read and write from it independently. NOTE: Extending classes are not
     * thread-safe and clients have to manage synchronization.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface HeapBuffer {

        /**
         * @param index that determines the position to read from
         * @return a read access for this buffer
         */
        ReadAccess getReadAccess(final ColumnDataIndex index);

        /**
         * @param index that determines the position to write to
         * @return a write access for this buffer
         */
        WriteAccess getWriteAccess(final ColumnDataIndex index);

        /**
         * @return the number of elements the buffer can store
         */
        int getCapacity();
    }

    private static final class DataSpecToHeapBufferMapper implements DataSpec.Mapper<HeapBuffer> {

        private final int m_capacity;

        private DataSpecToHeapBufferMapper(final int capacity) {
            m_capacity = capacity;
        }

        @Override
        public HeapBuffer visit(final StructDataSpec spec) {
            return new StructHeapBuffer(m_capacity, spec);
        }

        @Override
        public HeapBuffer visit(final DoubleDataSpec spec) {
            return new DoubleHeapBuffer(m_capacity);
        }

        @Override
        public HeapBuffer visit(final BooleanDataSpec spec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HeapBuffer visit(final IntDataSpec spec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HeapBuffer visit(final LongDataSpec spec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HeapBuffer visit(final VoidDataSpec spec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HeapBuffer visit(final VarBinaryDataSpec spec) {
            return new VarBinaryBuffer(m_capacity);
        }

        @Override
        public HeapBuffer visit(final ListDataSpec spec) {
            return new ListHeapBuffer(m_capacity, spec);
        }

        @Override
        public HeapBuffer visit(final StringDataSpec spec) {
            return new StringHeapBuffer(m_capacity);
        }

        @Override
        public HeapBuffer visit(final ByteDataSpec spec) {
            return new ByteHeapBuffer(m_capacity);
        }

        @Override
        public HeapBuffer visit(final FloatDataSpec spec) {
            return new FloatHeapBuffer(m_capacity);
        }

    }

    private abstract static class PrimitiveHeapBuffer<B> implements HeapBuffer {

        private final BitSet m_isMissing;

        protected final B m_values;

        private final int m_capacity;

        PrimitiveHeapBuffer(final int capacity, final IntFunction<B> arrayFactory) {
            m_values = arrayFactory.apply(capacity);
            m_isMissing = new BitSet(capacity);
            m_capacity = capacity;
        }

        @Override
        public final int getCapacity() {
            return m_capacity;
        }

        protected abstract class PrimitiveBufferReadAccess extends AbstractAccess implements ReadAccess {

            PrimitiveBufferReadAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public boolean isMissing() {
                return m_isMissing.get(getIdx());
            }
        }

        protected abstract class PrimitiveBufferWriteAccess<A extends ReadAccess> extends AbstractAccess
            implements WriteAccess {

            PrimitiveBufferWriteAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public final void setMissing() {
                m_isMissing.set(getIdx());
            }

            @SuppressWarnings("unchecked")
            @Override
            public final void setFrom(final ReadAccess access) {
                final int index = getIdx();
                if (access.isMissing()) {
                    m_isMissing.set(getIdx());
                } else {
                    onSet(index);
                    setFromNonMissing(index, (A)access);
                }
            }

            protected abstract void setFromNonMissing(final int index, final A access);

            protected void onSet(final int index) {
                m_isMissing.clear(index);
            }

        }

    }

    private static final class ByteHeapBuffer extends PrimitiveHeapBuffer<byte[]> {

        ByteHeapBuffer(final int capacity) {
            super(capacity, byte[]::new);
        }

        @Override
        public ReadAccess getReadAccess(final ColumnDataIndex index) {
            return new ByteBufferReadAccess(index);
        }

        @Override
        public WriteAccess getWriteAccess(final ColumnDataIndex index) {
            return new ByteBufferWriteAccess(index);
        }

        private final class ByteBufferReadAccess extends PrimitiveBufferReadAccess implements ByteReadAccess {

            ByteBufferReadAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public byte getByteValue() {
                return m_values[getIdx()];
            }

        }

        private final class ByteBufferWriteAccess extends PrimitiveBufferWriteAccess<ByteReadAccess>
            implements ByteWriteAccess {

            ByteBufferWriteAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public void setByteValue(final byte value) {
                int index = getIdx();
                m_values[index] = value;
                onSet(index);
            }

            @Override
            protected void setFromNonMissing(final int index, final ByteReadAccess access) {
                m_values[index] = access.getByteValue();
            }

        }

    }

    private static final class DoubleHeapBuffer extends PrimitiveHeapBuffer<double[]> {

        DoubleHeapBuffer(final int capacity) {
            super(capacity, double[]::new);
        }

        @Override
        public ReadAccess getReadAccess(final ColumnDataIndex index) {
            return new BufferDoubleReadAccess(index);
        }

        @Override
        public WriteAccess getWriteAccess(final ColumnDataIndex index) {
            return new BufferDoubleWriteAccess(index);
        }

        private final class BufferDoubleReadAccess extends PrimitiveBufferReadAccess implements DoubleReadAccess {

            BufferDoubleReadAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public double getDoubleValue() {
                return m_values[getIdx()];
            }

        }

        private final class BufferDoubleWriteAccess extends PrimitiveBufferWriteAccess<DoubleReadAccess>
            implements DoubleWriteAccess {

            BufferDoubleWriteAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public void setDoubleValue(final double value) {
                m_values[getIdx()] = value;
            }

            @Override
            protected void setFromNonMissing(final int index, final DoubleReadAccess access) {
                m_values[index] = access.getDoubleValue();
            }

        }

    }

    private static final class FloatHeapBuffer extends PrimitiveHeapBuffer<float[]> {

        FloatHeapBuffer(final int capacity) {
            super(capacity, float[]::new);
        }

        @Override
        public ReadAccess getReadAccess(final ColumnDataIndex index) {
            return new BufferFloatReadAccess(index);
        }

        @Override
        public WriteAccess getWriteAccess(final ColumnDataIndex index) {
            return new BufferFloatWriteAccess(index);
        }

        private final class BufferFloatReadAccess extends PrimitiveBufferReadAccess implements FloatReadAccess {

            BufferFloatReadAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public float getFloatValue() {
                return m_values[getIdx()];
            }

        }

        private final class BufferFloatWriteAccess extends PrimitiveBufferWriteAccess<FloatReadAccess>
            implements FloatWriteAccess {

            BufferFloatWriteAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public void setFloatValue(final float value) {
                m_values[getIdx()] = value;
            }

            @Override
            protected void setFromNonMissing(final int index, final FloatReadAccess access) {
                m_values[index] = access.getFloatValue();
            }

        }

    }

    private static final class StructHeapBuffer implements HeapBuffer {

        private final BitSet m_isMissing;

        private final HeapBuffer[] m_children;

        private final int m_capacity;

        StructHeapBuffer(final int capacity, final StructDataSpec spec) {
            m_isMissing = new BitSet(capacity);
            m_children = new HeapBuffer[spec.size()];
            var specMapper = new DataSpecToHeapBufferMapper(capacity);
            Arrays.setAll(m_children, i -> spec.getDataSpec(i).accept(specMapper));
            m_capacity = capacity;
        }

        @Override
        public int getCapacity() {
            return m_capacity;
        }

        @Override
        public ReadAccess getReadAccess(final ColumnDataIndex index) {
            return new StructBufferReadAccess(index);
        }

        @Override
        public WriteAccess getWriteAccess(final ColumnDataIndex index) {
            return new StructBufferWriteAccess(index);
        }

        private final class StructBufferReadAccess extends AbstractAccess implements StructReadAccess {

            private final ReadAccess[] m_childAccesses;

            StructBufferReadAccess(final ColumnDataIndex index) {
                super(index);
                m_childAccesses = Stream.of(m_children)//
                    .map(b -> b.getReadAccess(index))//
                    .toArray(ReadAccess[]::new);

            }

            @Override
            public boolean isMissing() {
                return m_isMissing.get(getIdx());
            }

            @Override
            public int size() {
                return m_children.length;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <R extends ReadAccess> R getAccess(final int index) {
                return (R)m_childAccesses[index];
            }

        }

        private final class StructBufferWriteAccess extends AbstractAccess implements StructWriteAccess {
            private final WriteAccess[] m_accesses;

            StructBufferWriteAccess(final ColumnDataIndex index) {
                super(index);
                m_accesses = Stream.of(m_children)//
                    .map(b -> b.getWriteAccess(index))//
                    .toArray(WriteAccess[]::new);
            }

            @Override
            public void setMissing() {
                m_isMissing.set(getIdx());
            }

            @Override
            public void setFrom(final ReadAccess access) {
                if (access.isMissing()) {
                    setMissing();
                } else {
                    var structAccess = (StructReadAccess)access;
                    for (int i = 0; i < m_accesses.length; i++) {//NOSONAR
                        m_accesses[i].setFrom(structAccess.getAccess(i));
                    }
                }
            }

            @Override
            public int size() {
                return m_accesses.length;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <W extends WriteAccess> W getWriteAccess(final int index) {
                return (W)m_accesses[index];
            }

        }

    }

    private static final class ListHeapBuffer implements HeapBuffer {

        private final BitSet m_isMissing;

        private final HeapBuffer[] m_buffers;

        private final DataSpec m_elementSpec;

        ListHeapBuffer(final int capacity, final DataSpec spec) {
            m_buffers = new HeapBuffer[capacity];
            m_isMissing = new BitSet(capacity);
            m_elementSpec = spec;
        }

        @Override
        public int getCapacity() {
            return m_buffers.length;
        }

        @Override
        public ReadAccess getReadAccess(final ColumnDataIndex index) {
            return new ListBufferReadAccess(index);
        }

        @Override
        public WriteAccess getWriteAccess(final ColumnDataIndex index) {
            return new ListBufferWriteAccess(index);
        }

        private final class ListBufferReadAccess extends AbstractAccess implements ListReadAccess {

            private final MutableColumnDataIndex m_elementIndex = new MutableColumnDataIndex();

            private int m_previousBufferIdx = -1;

            private final DelegatingReadAccess m_elementAccess =
                DelegatingReadAccesses.createDelegatingAccess(m_elementSpec);

            protected ListBufferReadAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public boolean isMissing() {
                return m_isMissing.get(getIdx());
            }

            @SuppressWarnings("unchecked")
            @Override
            public <R extends ReadAccess> R getAccess() {
                updateElementAccess();
                return (R)m_elementAccess;
            }

            // TODO is it enough to update the element access upon interactions with ListReadAccess or does it have
            // to happen on interactions with m_elementAccess?
            private void updateElementAccess() {
                int currentBufferIdx = getIdx();
                if (m_previousBufferIdx == currentBufferIdx) {
                    m_elementAccess.setDelegateAccess(m_buffers[currentBufferIdx].getReadAccess(m_elementIndex));
                    m_previousBufferIdx = currentBufferIdx;
                }
            }

            @Override
            public void setIndex(final int index) {
                updateElementAccess();
                m_elementIndex.setIndex(index);
            }

            @Override
            // TODO why do clients use this method instead of calling setIndex() and isMissing() on the element access?
            public boolean isMissing(final int index) {
                updateElementAccess();
                var prevIndex = m_elementIndex.getIndex();
                m_elementIndex.setIndex(index);
                try {
                    return getAccess().isMissing();
                } finally {
                    m_elementIndex.setIndex(prevIndex);
                }
            }

            @Override
            public int size() {
                updateElementAccess();
                return m_buffers[getIdx()].getCapacity();
            }

        }

        private final class ListBufferWriteAccess extends AbstractAccess implements ListWriteAccess {

            private final DelegatingWriteAccess m_elementAccess =
                DelegatingWriteAccesses.createDelegatingWriteAccess(m_elementSpec);

            private final MutableColumnDataIndex m_elementIndex = new MutableColumnDataIndex();

            private int m_previousBufferIndex = -1;

            protected ListBufferWriteAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public void setMissing() {
                m_isMissing.set(getIdx());
            }

            @Override
            public void setFrom(final ReadAccess access) {
                if (access.isMissing()) {
                    setMissing();
                } else {
                    var listAccess = (ListReadAccess)access;
                    int size = listAccess.size();
                    create(size);
                    var elementReadAccess = listAccess.getAccess();
                    var elementWriteAccess = getWriteAccess();
                    for (int i = 0; i < size; i++) {
                        listAccess.setIndex(i);
                        setWriteIndex(i);
                        elementWriteAccess.setFrom(elementReadAccess);
                    }
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public <W extends WriteAccess> W getWriteAccess() {
                updateElementAccess(getIdx());
                return (W)m_elementAccess;
            }

            @Override
            public void setWriteIndex(final int index) {
                m_elementIndex.setIndex(index);
            }

            @Override
            public void create(final int size) {
                int bufferIndex = getIdx();
                m_buffers[bufferIndex] = m_elementSpec.accept(new DataSpecToHeapBufferMapper(size));
                updateElementAccess(bufferIndex);
            }

            private void updateElementAccess(final int bufferIndex) {
                if (m_previousBufferIndex != bufferIndex) {
                    m_elementAccess.setDelegateAccess(m_buffers[bufferIndex].getWriteAccess(m_elementIndex));
                    m_previousBufferIndex = bufferIndex;
                }
            }

        }

    }

    private static final class MutableColumnDataIndex implements ColumnDataIndex {

        private int m_index = -1;

        @Override
        public int getIndex() {
            return m_index;
        }

        void setIndex(final int index) {
            m_index = index;
        }

    }

    private static final class StringHeapBuffer implements HeapBuffer {
        private final String[] m_buffer;

        StringHeapBuffer(final int capacity) {
            m_buffer = new String[capacity];
        }

        @Override
        public int getCapacity() {
            return m_buffer.length;
        }

        @Override
        public StringReadAccess getReadAccess(final ColumnDataIndex index) {
            return new StringBufferReadAccess(index);
        }

        @Override
        public StringWriteAccess getWriteAccess(final ColumnDataIndex index) {
            return new StringBufferWriteAccess(index);
        }

        private final class StringBufferWriteAccess extends AbstractAccess implements StringWriteAccess {

            StringBufferWriteAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public void setMissing() {
                m_buffer[getIdx()] = null;
            }

            @Override
            public void setFrom(final ReadAccess access) {
                setStringValue(((StringReadAccess)access).getStringValue());
            }

            @Override
            public void setStringValue(final String value) {
                m_buffer[getIdx()] = value;
            }

        }

        private final class StringBufferReadAccess extends AbstractAccess implements StringReadAccess {

            StringBufferReadAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public boolean isMissing() {
                return getStringValue() == null;
            }

            @Override
            public String getStringValue() {
                return m_buffer[getIdx()];
            }

        }

    }

    private abstract static class AbstractAccess {

        private final ColumnDataIndex m_index;

        protected AbstractAccess(final ColumnDataIndex index) {
            m_index = index;
        }

        protected int getIdx() {
            return m_index.getIndex();
        }
    }

    private static final class VarBinaryBuffer implements HeapBuffer {
        private final Object[] m_objects;

        private final ObjectSerializer<?>[] m_serializers;

        VarBinaryBuffer(final int capacity) {
            m_objects = new Object[capacity];
            m_serializers = new ObjectSerializer<?>[capacity];
        }

        @Override
        public int getCapacity() {
            return m_objects.length;
        }

        @Override
        public VarBinaryReadAccess getReadAccess(final ColumnDataIndex index) {
            return new VarBinaryBufferReadAccess(index);
        }

        @Override
        public VarBinaryWriteAccess getWriteAccess(final ColumnDataIndex index) {
            return new VarBinaryBufferWriteAccess(index);
        }

        private final class VarBinaryBufferReadAccess extends AbstractAccess implements VarBinaryReadAccess {

            VarBinaryBufferReadAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public boolean isMissing() {
                var index = getIdx();
                return m_objects[index] == null && m_serializers[index] == null;
            }

            @Override
            public byte[] getByteArray() {
                return getObject();
            }

            @Override
            public boolean hasObjectAndSerializer() {
                return getSerializer() != null;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T> T getObject() {
                return (T)m_objects[getIdx()];
            }

            @Override
            public ObjectSerializer<?> getSerializer() {
                return m_serializers[getIdx()];
            }

            @Override
            public <T> T getObject(final ObjectDeserializer<T> deserializer) {
                throw new UnsupportedOperationException();
            }

        }

        private final class VarBinaryBufferWriteAccess extends AbstractAccess implements VarBinaryWriteAccess {

            VarBinaryBufferWriteAccess(final ColumnDataIndex index) {
                super(index);
            }

            @Override
            public void setMissing() {
                var index = getIdx();
                m_objects[index] = null;
                m_serializers[index] = null;
            }

            @Override
            public void setFrom(final ReadAccess access) {
                if (access.isMissing()) {
                    setMissing();
                } else {
                    var binaryAccess = (VarBinaryReadAccess)access;
                    if (binaryAccess.hasObjectAndSerializer()) {
                        setObject(binaryAccess.getObject(), binaryAccess.getSerializer());
                    } else {
                        setByteArray(binaryAccess.getByteArray());
                    }
                }
            }

            @Override
            public void setByteArray(final byte[] value) {
                m_objects[getIdx()] = value;
            }

            @Override
            public void setByteArray(final byte[] array, final int index, final int length) {
                setByteArray(Arrays.copyOfRange(array, index, index + length));
            }

            @Override
            public <T> void setObject(final T value, final ObjectSerializer<T> serializer) {
                var index = getIdx();
                m_objects[index] = value;
                m_serializers[index] = serializer;
            }

        }

    }
}