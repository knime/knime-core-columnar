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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 * Arrow implementation of {@link StringWriteData} and {@link StringReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OnHeapArrowSmartStringData {

    private OnHeapArrowSmartStringData() {
    }

    interface CopyableStringWriteData extends StringWriteData {
        void setStringBytes(int index, byte[] val);

        void setFrom(CopyableStringReadData sourceData, int sourceIndex, int targetIndex);
    }

    interface CopyableStringReadData extends StringReadData {
        byte[] getStringBytesNullable(int index);
    }

    // TODO(AP-23864) to make state handling of this object simpler, we could split it into two classes:
    // one for writing and one for reading. A superclass could hold the data structure and the subclasses could limit
    // the access to the data structure to either writing or reading.
    /**
     * Holds the data of a string column in a combination of a string array and a byte array.
     *
     * Note that none of the two representations is guaranteed to be complete to the last set index.
     *
     * The byte[] is complete up to a certain index (the last one that was encoded) ({@link #lastWrittenBytesIndex()})
     * which is tracked by the offsets buffer. The String[] is sparsely filled. However, it is guaranteed to contain all
     * values after the {@link #lastWrittenBytesIndex()}.
     */
    private static final class LazyEncodedStringList {

        /*
         * Thread safety considerations:
         * - TODO(AP-23864) In `getString` we could decode the same String twice which would result in a wrong estimate
         *   in totalDataSizeInBytes
         * - We only set values in m_dataBytes (and m_offsets) during writing (which is not thread-safe anyway) and when
         *   encoding the data to copy it to a vector. This happens after writing is done.
         */

        private String[] m_data;

        private final ByteArrayList m_dataBytes;

        private final OffsetBuffer m_offsets;

        // Size of m_data in bytes
        private long m_totalDataSizeInBytes = 0;

        // Note that this might be `null`. In this case, this object only allows reading and not writing
        private LargeOffsetBuffer m_accumulatedDataSizeInBytes;

        public LazyEncodedStringList(final int capacity) {
            m_data = new String[capacity];
            m_dataBytes = new ByteArrayList();
            m_offsets = new OffsetBuffer(capacity);
            m_accumulatedDataSizeInBytes = new LargeOffsetBuffer(capacity);
        }

        private LazyEncodedStringList(final String[] data, final ByteArrayList byteArrayList,
            final OffsetBuffer offsets) {
            m_data = data;
            m_dataBytes = byteArrayList;
            m_offsets = offsets;
        }

        public int capacity() {
            return m_data.length;
        }

        /**
         * Encodes the strings in the range <code>[lastWrittenBytesIndex() + 1, endIdx)</code> into the byte array
         *
         * @param endIdx the next index after the last string to encode
         */
        private void encode(final int endIdx) {
            for (var i = m_offsets.lastWrittenIndex() + 1; i < endIdx; i++) {
                var str = m_data[i];
                if (str != null) {
                    // TODO(AP-23858) Is CharsetEncoder faster?
                    // TODO(AP-23858) Can encode directly to m_dataBytes without a temporary byte[]?
                    var bytes = str.getBytes(StandardCharsets.UTF_8);
                    var dataIndex = m_offsets.add(i, bytes.length);
                    m_dataBytes.addElements(dataIndex.start(), bytes);
                } else {
                    m_offsets.add(i, 0);
                }
            }
        }

        // Writing elements

        public void setString(final int index, final String value) {
            m_data[index] = value;
            var memorySize = StringSizeUtils.memorySize(value);
            m_accumulatedDataSizeInBytes.add(index, memorySize);
            m_totalDataSizeInBytes += memorySize;
        }

        public void setNumElements(final int numElements) {
            m_data = Arrays.copyOf(m_data, numElements);
            m_offsets.setNumElements(numElements);
            m_accumulatedDataSizeInBytes.setNumElements(numElements);
            m_totalDataSizeInBytes = m_accumulatedDataSizeInBytes.getNumData(numElements);
        }

        /**
         * Copy the data to the given vector. Allocates the necessary memory.
         *
         * @param vector the vector to copy the data to
         */
        @SuppressWarnings("resource") // The buffers are owned by the vector and closed with it
        public void copyTo(final BaseVariableWidthVector vector) {
            encode(capacity());
            vector.allocateNew(m_dataBytes.size(), capacity());
            vector.getDataBuffer().setBytes(0, m_dataBytes.elements(), 0, m_dataBytes.size());
            m_offsets.copyTo(vector.getOffsetBuffer());
        }

        public void setStringBytes(final int index, final byte[] val) {
            // Note - we prefer to encode the previous values to decoding this one, because
            // - it is likely that we need the byte representation of the previous values anyway
            // - we have two representations of the data in the newer data object, so this will be more flexible and
            //   efficient to work with

            // Encode the values that not yet encoded
            encode(index);

            // Copy over the new value
            var dataIndex = m_offsets.add(index, val.length);
            System.arraycopy(val, 0, m_dataBytes, dataIndex.start(), val.length);
        }

        public void setFrom(final LazyEncodedStringList sourceList, final int sourceIndex, final int targetIndex) {
            // Copy the string
            // Note that this can be null if the data was just read and not yet decoded. In this case, the
            // bytes will be set and the string will be decoded on access
            m_data[targetIndex] = sourceList.m_data[sourceIndex];
            var memorySize = StringSizeUtils.memorySize(m_data[targetIndex]);
            m_accumulatedDataSizeInBytes.add(targetIndex, memorySize);
            m_totalDataSizeInBytes += memorySize;

            // Copy the bytes if they are present (must be present if the string is null)
            if (sourceList.m_offsets.lastWrittenIndex() >= sourceIndex) {
                var sourceDataIndex = sourceList.m_offsets.get(sourceIndex);
                var targetDataIndex = m_offsets.add(targetIndex, sourceDataIndex.length());
                System.arraycopy(sourceList.m_dataBytes, sourceDataIndex.start(), m_dataBytes, targetDataIndex.start(),
                    sourceDataIndex.length());
            }
        }

        // Accessing elements

        public String getString(final int index) {
            if (m_data[index] == null) {
                // Decode the string
                // TODO(AP-23858) Is CharsetDecoder faster?
                var dataIndex = m_offsets.get(index);
                var val =
                    new String(m_dataBytes.elements(), dataIndex.start(), dataIndex.length(), StandardCharsets.UTF_8);

                m_data[index] = val;
                // Note: we do not update the accumulated size here because we would need to re-compute the accumulated
                // size for all following indices. This is not a problem, because we are not using the accumulated size
                // anymore, because we are done writing and only reading now.
                m_totalDataSizeInBytes += StringSizeUtils.memorySize(val);
            }

            return m_data[index];
        }

        public byte[] getStringBytesNullable(final int index) {
            if (m_offsets.lastWrittenIndex() >= index) {
                var dataIndex = m_offsets.get(index);
                var bytes = new byte[dataIndex.length()];
                System.arraycopy(m_dataBytes, dataIndex.start(), bytes, 0, bytes.length);
                return bytes;
            } else {
                return null;
            }
        }

        public static LazyEncodedStringList createFrom(final ArrowBuf dataBuffer, final ArrowBuf offsetsBuffer,
            final int numElements) {
            var offsets = OffsetBuffer.createFrom(offsetsBuffer, numElements);
            var dataBytes = new byte[offsets.getNumData(numElements)];
            dataBuffer.getBytes(0, dataBytes);
            return new LazyEncodedStringList(new String[numElements], new ByteArrayList(dataBytes), offsets);
        }
    }

    /**
     * Holds the data of a string column in a combination of a string array and a byte array. Encoding is done lazily.
     */
    public static final class ArrowStringWriteData extends
        AbstractOnHeapArrowWriteData<LazyEncodedStringList, ArrowStringReadData> implements CopyableStringWriteData {

        private ArrowStringWriteData(final int capacity) {
            super(new LazyEncodedStringList(capacity), capacity);
        }

        private ArrowStringWriteData(final LazyEncodedStringList data, final ValidityBuffer validity,
            final int offset) {
            super(data, validity, offset, data.capacity());
        }

        @Override
        public void setString(final int index, final String val) {
            m_data.setString(index + m_offset, val);
            setValid(index);
        }

        // >>> LOW LEVEL COPY

        @Override
        public void setStringBytes(final int index, final byte[] val) {
            m_data.setStringBytes(index + m_offset, val);
            setValid(index);
        }

        @Override
        public void setFrom(final CopyableStringReadData sourceData, final int sourceIndex, final int targetIndex) {
            if (sourceData instanceof ArrowStringReadData sourceArrowData) {
                if (sourceArrowData.isMissing(sourceIndex)) {
                    setMissing(targetIndex + m_offset);
                } else {
                    m_data.setFrom(sourceArrowData.m_data, sourceIndex, targetIndex + m_offset);
                }
            }
        }

        // <<< LOW LEVEL COPY

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return new ArrowStringWriteData(m_data, m_validity, m_offset + start);
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return m_data.m_accumulatedDataSizeInBytes.getNumData(numElements) // m_data.m_data
                + m_data.m_dataBytes.size() // m_data.m_dataBytes
                + OffsetBuffer.usedSizeFor(m_data.capacity()) // m_data.m_offsets
                + m_validity.sizeOf();
        }

        @Override
        public long sizeOf() {
            return m_data.m_totalDataSizeInBytes // m_data.m_data
                + m_data.m_dataBytes.size() // m_data.m_dataBytes
                + m_data.m_offsets.sizeOf() // m_data.m_offsets
                + m_data.m_accumulatedDataSizeInBytes.sizeOf() // m_data.m_accumulatedDataSizeInBytes
                + m_validity.sizeOf();
        }

        @Override
        protected ArrowStringReadData createReadData(final int length) {
            m_data.m_accumulatedDataSizeInBytes = null;
            // note the offsets reflect the current state of the dataBytes (not necessarily all strings of this data)
            return new ArrowStringReadData(m_data, m_validity);
        }

        @Override
        protected void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_data.setNumElements(numElements);
        }
    }

    /** Arrow implementation of {@link StringReadData}. */
    public static final class ArrowStringReadData extends AbstractOnHeapArrowReadData<LazyEncodedStringList>
        implements CopyableStringReadData {

        private ArrowStringReadData(final LazyEncodedStringList data, final ValidityBuffer validity) {
            super(data, validity, data.capacity());
        }

        private ArrowStringReadData(final LazyEncodedStringList data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(data, validity, offset, length);
        }

        @Override
        public String getString(final int index) {
            // Assuming not missing (the caller has to check)
            return m_data.getString(index + m_offset);
        }

        @Override
        public byte[] getStringBytesNullable(final int index) {
            if (!isMissing(index + m_offset)) {
                return m_data.getStringBytesNullable(index + m_offset);
            } else {
                return null;
            }
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return new ArrowStringReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return m_data.m_totalDataSizeInBytes // m_data.m_data
                + m_data.m_dataBytes.size() // m_data.m_dataBytes
                + m_data.m_offsets.sizeOf() // m_data.m_offsets
                + m_validity.sizeOf();
        }
    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowSmartStringData} */
    public static final class ArrowStringDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowStringDataFactory} */
        public static final ArrowStringDataFactory INSTANCE = new ArrowStringDataFactory();

        private ArrowStringDataFactory() {
            super(0);
        }

        @Override
        public ArrowStringWriteData createWrite(final int capacity) {
            return new ArrowStringWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.VARCHAR.getType());
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public void copyToVector(final NullableReadData data, final FieldVector fieldVector) {
            var d = (ArrowStringReadData)data;
            d.checkNotSliced();
            var vector = (VarCharVector)fieldVector;

            d.m_data.copyTo(vector);
            d.m_validity.copyTo(vector.getValidityBuffer());

            vector.setLastSet(d.length() - 1);
            vector.setValueCount(d.length());
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public ArrowStringReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {

                var valueCount = vector.getValueCount();
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);
                var data =
                    LazyEncodedStringList.createFrom(vector.getDataBuffer(), vector.getOffsetBuffer(), valueCount);

                return new ArrowStringReadData(data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowStringData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return Long.BYTES // accumulated data size
                + Integer.BYTES // offsets
                + Integer.BYTES // Compressed OOPs in the data String[]
                + Byte.BYTES; // validity
        }
    }
}
