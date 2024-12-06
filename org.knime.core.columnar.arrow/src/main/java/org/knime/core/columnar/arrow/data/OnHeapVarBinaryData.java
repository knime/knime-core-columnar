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
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.OffsetsBuffer.IntDataIndex;
import org.knime.core.columnar.arrow.data.OffsetsBuffer.LongDataIndex;
import org.knime.core.columnar.arrow.data.OffsetsBuffer.LongOffsetsReadBuffer;
import org.knime.core.columnar.arrow.data.OffsetsBuffer.LongOffsetsWriteBuffer;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OnHeapVarBinaryData {

    public static final Factory FACTORY = new Factory();

    private OnHeapVarBinaryData() {
    }

    // TODO Remove this method. We should support more than Integer.MAX_VALUE bytes.
    private static IntDataIndex offsetsToInt(final LongDataIndex offsets) {
        return new IntDataIndex((int)offsets.start(), (int)offsets.end());
    }

    /** Implementation of {@link VarBinaryWriteData} that writes directly into the flattened byte array. */
    public static final class OnHeapVarBinaryWriteData extends AbstractArrowWriteData implements VarBinaryWriteData {

        private byte[] m_data;

        private LongOffsetsWriteBuffer m_offsets;

        private int m_capacity;

        private OnHeapVarBinaryWriteData(final int capacity) {
            super(capacity);
            m_capacity = capacity;
            m_offsets = OffsetsBuffer.createLongWriteBuffer(capacity);
            m_data = new byte[capacity * 32]; // initial estimate
        }

        private OnHeapVarBinaryWriteData(final int offset, final byte[] data, final LongOffsetsWriteBuffer offsets,
            final ValidityBuffer validity, final int capacity) {
            super(offset, validity);
            m_data = data;
            m_offsets = offsets;
            m_capacity = capacity;
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            var dataIndex = offsetsToInt(m_offsets.add(m_offset + index, val.length));

            // Ensure capacity of data array
            ensureDataCapacity(dataIndex.end());

            // Copy val into data array
            System.arraycopy(val, 0, m_data, dataIndex.start(), val.length);

            // Set validity bit
            setValid(m_offset + index);
        }

        @Override
        public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
            // TODO support more than Integer.MAX_VALUE bytes
            int startIndex = (int)m_offsets.getStartIndex(m_offset + index);

            // Create a custom DataOutput that writes directly into m_data
            var dataOutput = new ByteArrayDataOutput(m_data, startIndex) {
                @Override
                protected void ensureCapacity(final int newLength) {
                    OnHeapVarBinaryWriteData.this.ensureDataCapacity(newLength);
                    m_data = OnHeapVarBinaryWriteData.this.m_data; // Update reference in case array was reallocated
                }
            };

            try {
                serializer.serialize(dataOutput, value);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing object", e);
            }

            // Add offset to offsets buffer
            var elementLength = ((ByteArrayDataOutput)dataOutput).getPosition() - startIndex;
            m_offsets.add(m_offset + index, elementLength);

            // Set validity bit
            setValid(m_offset + index);
        }

        private void ensureDataCapacity(final int requiredCapacity) {
            if (requiredCapacity > m_data.length) {
                // TODO is this smart? should we use a different strategy?
                // Expand the data array
                int newCapacity = Math.max(requiredCapacity, m_data.length * 2);
                byte[] newData = new byte[newCapacity];
                System.arraycopy(m_data, 0, newData, 0, m_data.length);
                m_data = newData;
            }
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (minimumCapacity > m_capacity) {
                m_validity.setNumElements(minimumCapacity);
                m_offsets.setNumElements(minimumCapacity);
                m_capacity = minimumCapacity;
            }
        }

        @Override
        public int capacity() {
            return m_capacity;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return m_data.length + OffsetsBuffer.usedLongSizeFor(numElements + 1)
                + ValidityBuffer.usedSizeFor(numElements);
        }

        @Override
        public long sizeOf() {
            return usedSizeFor(m_capacity);
        }

        @Override
        public OnHeapVarBinaryReadData close(final int length) {
            m_validity.setNumElements(length);
            m_offsets.setNumElements(length);
            var readOffsets = m_offsets.close();
            // Trim the data array to the actual data length (TODO is this necessary?)
            byte[] data = new byte[offsetsToInt(readOffsets.get(length - 1)).end()];
            System.arraycopy(m_data, 0, data, 0, data.length);
            return new OnHeapVarBinaryReadData(data, m_offsets.close(), m_validity, length);
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new OnHeapVarBinaryWriteData(m_offset + start, m_data, m_offsets, m_validity, m_capacity - start);
        }

        @Override
        protected void closeResources() {
            // Nothing to do
        }
    }

    /** Implementation of {@link VarBinaryReadData} that reads directly from the flattened byte array. */
    public static final class OnHeapVarBinaryReadData extends AbstractArrowReadData implements VarBinaryReadData {

        private final byte[] m_data;

        private final LongOffsetsReadBuffer m_offsets;

        private final ValidityBuffer m_validity;

        private OnHeapVarBinaryReadData(final byte[] data, final LongOffsetsReadBuffer offsets,
            final ValidityBuffer validity, final int length) {
            super(validity, length);
            m_data = data;
            m_offsets = offsets;
            m_validity = validity;
        }

        private OnHeapVarBinaryReadData(final byte[] data, final LongOffsetsReadBuffer offsets,
            final ValidityBuffer validity, final int offset, final int length) {
            super(validity, offset, length);
            m_data = data;
            m_offsets = offsets;
            m_validity = validity;
        }

        @Override
        public byte[] getBytes(final int index) {
            var dataIndex = offsetsToInt(m_offsets.get(m_offset + index));
            int length = dataIndex.end() - dataIndex.start();
            byte[] val = new byte[length];
            System.arraycopy(m_data, dataIndex.start(), val, 0, length);
            return val;
        }

        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            int idx = m_offset + index;
            var dataIndex = offsetsToInt(m_offsets.get(idx));
            var dataInput = new ByteArrayDataInput(m_data, dataIndex.start(), dataIndex.end());

            try {
                return deserializer.deserialize(dataInput);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing object", e);
            }
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new OnHeapVarBinaryReadData(m_data, m_offsets, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return m_data.length + OffsetsBuffer.usedLongSizeFor(m_length) + m_validity.sizeOf();
        }

        @Override
        protected void closeResources() {
            // Nothing to do
        }
    }

    /** Factory class for {@link OnHeapVarBinaryData}. */
    public static final class Factory extends AbstractArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private Factory() {
            super(ArrowColumnDataFactoryVersion.version(CURRENT_VERSION));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, LargeBinary.INSTANCE);
        }

        @Override
        public ArrowWriteData createWrite(final int capacity) {
            return new OnHeapVarBinaryWriteData(capacity);
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                int valueCount = vector.getValueCount();
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);
                var offsets = OffsetsBuffer.createLongReadBuffer(vector.getOffsetBuffer(), valueCount);
                byte[] data = new byte[(int)vector.getDataBuffer().capacity()];
                vector.getDataBuffer().getBytes(0, data);
                return new OnHeapVarBinaryReadData(data, offsets, validity, valueCount);
            } else {
                throw new IOException("Cannot read OnHeapVarBinaryData with version " + version + ". Current version: "
                    + m_version + ".");
            }
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            var d = (OnHeapVarBinaryReadData)data;
            var v = (LargeVarBinaryVector)vector;

            // Make sure the buffers are allocated with the correct size
            v.allocateNew(d.m_data.length, d.length());

            v.getDataBuffer().setBytes(0, d.m_data, 0, d.m_data.length);
            d.m_offsets.copyTo(v.getOffsetBuffer());
            d.m_validity.copyTo(v.getValidityBuffer());

            v.setLastSet(d.length() - 1);
            v.setValueCount(d.length());
        }

        @Override
        public int initialNumBytesPerElement() {
            // TODO, what about the offset and validity buffer?
            return 32;
        }
    }
}
