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

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
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

    /** Implementation of {@link VarBinaryWriteData} that writes directly into the flattened byte array. */
    public static final class OnHeapVarBinaryWriteData extends AbstractArrowWriteData implements VarBinaryWriteData {

        private byte[] m_data;

        private OffsetsBuffer m_offsets;

        private int m_capacity;

        private int m_dataCapacity;

        private int m_dataLength;

        private OnHeapVarBinaryWriteData(final int capacity) {
            super(capacity);
            m_capacity = capacity;
            m_offsets = new OffsetsBuffer(capacity + 1); // Need one more for the last offset
            m_dataCapacity = capacity * 32; // initial estimate
            m_data = new byte[m_dataCapacity];
            m_dataLength = 0;
            m_offsets.putValue(0, 0); // initial offset
        }

        private OnHeapVarBinaryWriteData(final int offset, final byte[] data, final OffsetsBuffer offsets,
            final ValidityBuffer validity, final int capacity, final int dataLength) {
            super(offset, validity);
            m_data = data;
            m_offsets = offsets;
            m_capacity = capacity;
            m_dataCapacity = data.length;
            m_dataLength = dataLength;
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            // Ensure capacity of data array
            int requiredCapacity = m_dataLength + val.length;
            ensureDataCapacity(requiredCapacity);

            // Copy val into data array
            System.arraycopy(val, 0, m_data, m_dataLength, val.length);

            // Update offsets
            int startOffset = m_dataLength;
            m_dataLength += val.length;
            m_offsets.putValue(m_offset + index + 1, m_dataLength);

            // Set validity bit
            setValid(m_offset + index);
        }

        @Override
        public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
            int startOffset = m_dataLength;

            // Create a custom DataOutput that writes directly into m_data
            DataOutput dataOutput = new ByteArrayDataOutput(m_data, startOffset) {
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

            int endOffset = ((ByteArrayDataOutput)dataOutput).getPosition();

            m_dataLength = endOffset;
            m_offsets.putValue(m_offset + index + 1, endOffset);

            // Set validity bit
            setValid(m_offset + index);
        }

        private void ensureDataCapacity(final int requiredCapacity) {
            if (requiredCapacity > m_dataCapacity) {
                // Expand the data array
                int newCapacity = Math.max(requiredCapacity, m_dataCapacity * 2);
                byte[] newData = new byte[newCapacity];
                System.arraycopy(m_data, 0, newData, 0, m_dataLength);
                m_data = newData;
                m_dataCapacity = newCapacity;
            }
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (minimumCapacity > m_capacity) {
                m_validity.setNumElements(minimumCapacity);
                m_offsets.setNumElements(minimumCapacity + 1); // +1 for last offset
                m_capacity = minimumCapacity;
            }
        }

        @Override
        public int capacity() {
            return m_capacity;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return m_dataLength + OffsetsBuffer.usedSizeFor(numElements + 1) + ValidityBuffer.usedSizeFor(numElements);
        }

        @Override
        public long sizeOf() {
            return usedSizeFor(m_capacity);
        }

        @Override
        public OnHeapVarBinaryReadData close(final int length) {
            m_validity.setNumElements(length);
            m_offsets.endBuffer(length + 1); // +1 for last offset
            // Trim the data array to the actual data length
            byte[] data = new byte[m_dataLength];
            System.arraycopy(m_data, 0, data, 0, m_dataLength);
            return new OnHeapVarBinaryReadData(data, m_offsets, m_validity, length);
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new OnHeapVarBinaryWriteData(m_offset + start, m_data, m_offsets, m_validity, m_capacity - start,
                m_dataLength);
        }

        @Override
        protected void closeResources() {
            // Nothing to do
        }
    }

    /** Implementation of {@link VarBinaryReadData} that reads directly from the flattened byte array. */
    public static final class OnHeapVarBinaryReadData extends AbstractArrowReadData implements VarBinaryReadData {

        private final byte[] m_data;

        private final OffsetsBuffer m_offsets;

        private final ValidityBuffer m_validity;

        private OnHeapVarBinaryReadData(final byte[] data, final OffsetsBuffer offsets, final ValidityBuffer validity,
            final int length) {
            super(validity, length);
            m_data = data;
            m_offsets = offsets;
            m_validity = validity;
        }

        private OnHeapVarBinaryReadData(final byte[] data, final OffsetsBuffer offsets, final ValidityBuffer validity,
            final int offset, final int length) {
            super(validity, offset, length);
            m_data = data;
            m_offsets = offsets;
            m_validity = validity;
        }

        @Override
        public byte[] getBytes(final int index) {
            int idx = m_offset + index;
            int start = m_offsets.getStartIndex(idx);
            int end = m_offsets.getEndIndex(idx);
            int length = end - start;
            byte[] val = new byte[length];
            System.arraycopy(m_data, start, val, 0, length);
            return val;
        }

        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            int idx = m_offset + index;
            int start = m_offsets.getStartIndex(idx);
            int end = m_offsets.getEndIndex(idx);
            var dataInput = new ByteArrayDataInput(m_data, start, end);

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
            return m_data.length + m_offsets.sizeOf() + m_validity.sizeOf();
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
            return Field.nullable(name, MinorType.VARBINARY.getType());
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
                ValidityBuffer validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);
                OffsetsBuffer offsets = OffsetsBuffer.createFrom(vector.getOffsetBuffer(), valueCount + 1); // +1 for last offset
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

            v.setInitialCapacity(d.length());
            v.allocateNew();

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
