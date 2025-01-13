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
package org.knime.core.columnar.arrow.onheap.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OffsetsBuffer.LongOffsetsBuffer;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

import com.google.common.base.Preconditions;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.bytes.ByteBigArrayBigList;
import it.unimi.dsi.fastutil.bytes.ByteBigArrays;

/**
 * Arrow implementation of {@link VarBinaryWriteData} and {@link VarBinaryReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowVarBinaryData {

    /** The initial number of bytes allocated for each element. More memory is allocated when needed. */
    private static final int INITAL_BYTES_PER_ELEMENT = 32;

    private ArrowVarBinaryData() {
    }

    /** Arrow implementation of {@link VarBinaryWriteData}. */
    public static final class ArrowVarBinaryWriteData extends AbstractArrowWriteData implements VarBinaryWriteData {

        private ByteBigArrayBigList m_data;

        private LongOffsetsBuffer m_offsets;

        private int m_capacity;

        private ArrowVarBinaryWriteData(final int capacity) {
            super(capacity);
            m_capacity = capacity;
            m_offsets = new OffsetsBuffer.LongOffsetsBuffer(capacity);
            m_data = new ByteBigArrayBigList(capacity * (long)INITAL_BYTES_PER_ELEMENT);
        }

        private ArrowVarBinaryWriteData(final int offset, final ByteBigArrayBigList data,
            final LongOffsetsBuffer offsets, final ValidityBuffer validity, final int capacity) {
            super(offset, validity);
            m_data = data;
            m_offsets = offsets;
            m_capacity = capacity;
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            var dataIndex = m_offsets.add(m_offset + index, val.length);

            // Copy val into data array
            var bigVal = BigArrays.wrap(val); // Note that this wraps the array if it fits the segment size
            m_data.addElements(m_data.size64(), bigVal);

            // Set validity bit
            setValid(m_offset + index);
        }

        @Override
        public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
            // Start after all data that has been written so far
            long startIndex = m_offsets.getNumData(m_offset + index);

            // Create a custom DataOutput that writes directly into m_data
            var dataOutput = new ByteBigListDataOutput(m_data, startIndex);

            try {
                serializer.serialize(dataOutput, value);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing object", e);
            }

            // Add offset to offsets buffer
            var elementLength = dataOutput.getPosition() - startIndex;
            m_offsets.add(m_offset + index, elementLength);

            // Set validity bit
            setValid(m_offset + index);
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowVarBinaryWriteData(m_offset + start, m_data, m_offsets, m_validity, m_capacity - start);
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (minimumCapacity > m_capacity) {
                setNumElements(minimumCapacity);
                m_capacity = minimumCapacity;
            }
        }

        @Override
        public int capacity() {
            return m_capacity;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return ValidityBuffer.usedSizeFor(numElements) //
                + OffsetsBuffer.usedIntSizeFor(numElements) //
                + m_offsets.getNumData(numElements);
        }

        @Override
        public long sizeOf() {
            return m_validity.sizeOf() + OffsetsBuffer.usedIntSizeFor(m_capacity) + BigArrays.length(m_data.elements());
        }

        @Override
        public ArrowVarBinaryReadData close(final int length) {
            setNumElements(length);
            m_offsets.fillWithZeroLength();

            // Trim the data array to the actual data length
            var data = BigArrays.trim(m_data.elements(), m_offsets.getNumData(length));
            return new ArrowVarBinaryReadData(data, m_offsets, m_validity, length);
        }

        @Override
        protected void closeResources() {
            // Nothing to do
        }

        private void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_offsets.setNumElements(numElements);
        }
    }

    /** Arrow implementation of {@link VarBinaryReadData}. */
    public static final class ArrowVarBinaryReadData extends AbstractArrowReadData implements VarBinaryReadData {

        private final byte[][] m_data;

        private final LongOffsetsBuffer m_offsets;

        private ArrowVarBinaryReadData(final byte[][] data, final LongOffsetsBuffer offsets,
            final ValidityBuffer validity, final int length) {
            super(validity, length);
            m_data = data;
            m_offsets = offsets;
        }

        private ArrowVarBinaryReadData(final byte[][] data, final LongOffsetsBuffer offsets,
            final ValidityBuffer validity, final int offset, final int length) {
            super(validity, offset, length);
            m_data = data;
            m_offsets = offsets;
        }

        @Override
        public byte[] getBytes(final int index) {
            var dataIndex = m_offsets.get(m_offset + index);
            long length = dataIndex.end() - dataIndex.start();
            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Data length exceeds maximum integer value");
            }
            byte[] val = new byte[(int)length];
            BigArrays.copyFromBig(m_data, dataIndex.start(), val, 0, (int)length);
            return val;
        }

        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            int idx = m_offset + index;
            var dataIndex = m_offsets.get(idx);
            var dataInput = new ByteBigArrayDataInput(m_data, dataIndex.start(), dataIndex.end());

            try {
                return deserializer.deserialize(dataInput);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing object", e);
            }
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowVarBinaryReadData(m_data, m_offsets, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return BigArrays.length(m_data) + OffsetsBuffer.usedLongSizeFor(m_length) + m_validity.sizeOf();
        }

        @Override
        protected void closeResources() {
            // Nothing to do
        }
    }

    /**
     * Only used for backwards compatibility of ZonedDateTime data that used to store the zone id as dict encoded var
     * binary.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class ArrowDictEncodedLegacyDateTimeVarBinaryReadData extends AbstractArrowReadData
        implements VarBinaryReadData {

        private final int[] m_indices;

        private final ArrowVarBinaryReadData m_dictionary;

        private ArrowDictEncodedLegacyDateTimeVarBinaryReadData(final int[] indices,
            final ArrowVarBinaryReadData dictionary, final ValidityBuffer validity) {
            super(validity, indices.length);
            m_indices = indices;
            m_dictionary = dictionary;
        }

        private ArrowDictEncodedLegacyDateTimeVarBinaryReadData(final int[] indices,
            final ArrowVarBinaryReadData dictionary, final ValidityBuffer validity, final int offset,
            final int length) {
            super(validity, offset, length);
            m_indices = indices;
            m_dictionary = dictionary;
        }

        @Override
        public long sizeOf() {
            return m_validity.sizeOf() + m_indices.length * Integer.BYTES + m_dictionary.sizeOf();
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowDictEncodedLegacyDateTimeVarBinaryReadData(m_indices, m_dictionary, m_validity,
                m_offset + start, length);
        }

        @Override
        public byte[] getBytes(final int index) {
            return m_dictionary.getBytes(m_indices[m_offset + index]);
        }

        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            return m_dictionary.getObject(m_indices[m_offset + index], deserializer);
        }

    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link ArrowVarBinaryData} */
    public static final class ArrowVarBinaryDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowVarBinaryDataFactory} */
        public static final ArrowVarBinaryDataFactory INSTANCE = new ArrowVarBinaryDataFactory();

        /**
         * In version 0 we implemented dict encoding in order to load ZonedDateTime data in a backwards compatible way.
         * Newer versions don't support dict encoding because they should use our own struct dict encoding.
         */
        private static final ArrowColumnDataFactoryVersion V0 = ArrowColumnDataFactoryVersion.version(0);

        private ArrowVarBinaryDataFactory() {
            super(1);
        }

        @Override
        public ArrowWriteData createWrite(final int capacity) {
            return new ArrowVarBinaryWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, LargeBinary.INSTANCE);
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            var d = (ArrowVarBinaryReadData)data;
            var v = (LargeVarBinaryVector)vector;

            // Make sure the buffers are allocated with the correct size
            v.allocateNew(BigArrays.length(d.m_data), d.length());

            MemoryCopyUtils.copy(d.m_data, v.getDataBuffer());
            d.m_offsets.copyTo(v.getOffsetBuffer());
            d.m_validity.copyTo(v.getValidityBuffer());

            v.setLastSet(d.length() - 1);
            v.setValueCount(d.length());
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (V0.equals(version)) {
                var dictionaryEncoding = vector.getField().getDictionary();
                if (dictionaryEncoding != null) {
                    var dict = provider.lookup(dictionaryEncoding.getId());
                    Preconditions.checkArgument(dict.getVectorType().equals(MinorType.LARGEVARBINARY.getType()),
                        "Encountered dictionary vector of type '%s' but expected a LargeVarBinaryVector.");

                    var dictionary = readFromVector(dict.getVector());

                    // Read indices
                    var valueCount = vector.getValueCount();
                    var indices = new int[valueCount];
                    MemoryCopyUtils.copy(vector.getDataBufferAddress(), indices);
                    var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                    return new ArrowDictEncodedLegacyDateTimeVarBinaryReadData(indices, dictionary, validity);
                } else {
                    return readFromVector(vector);
                }
            } else if (m_version.equals(version)) {
                return readFromVector(vector);
            } else {
                throw new IOException(
                    "Cannot read ArrowVarBinaryData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        private static ArrowVarBinaryReadData readFromVector(final FieldVector vector) {

            int valueCount = vector.getValueCount();
            var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);
            var offsets = OffsetsBuffer.createLongBuffer(vector.getOffsetBuffer(), valueCount);

            // TODO use last offset instead of capacity?
            var data = ByteBigArrays.newBigArray(vector.getDataBuffer().capacity());
            MemoryCopyUtils.copy(vector.getDataBuffer(), data);

            return new ArrowVarBinaryReadData(data, offsets, validity, valueCount);
        }

        @Override
        public int initialNumBytesPerElement() {
            return INITAL_BYTES_PER_ELEMENT // data buffer
                + Long.BYTES // offset buffer
                + 1; // validity bit
        }
    }
}
