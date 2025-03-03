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
 *   Nov 30, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.LongSupplier;

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

/**
 * Arrow implementation of {@link StringWriteData} and {@link StringReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowStringData {

    private OnHeapArrowStringData() {
    }

    /** A list of strings that keeps track of the size of the data. */
    private static final class SizeAwareStringList {

        private String[] m_data;

        /**
         * The memory usage of the data in bytes. Remembers the accumulated size at each position of the array. Note
         * that we use 32-bit offsets when writing the data. However, we might consume more memory than the written
         * UTF-8 representation because Java Strings use UTF-16 encoding internally.
         */
        private LargeOffsetBuffer m_accumulatedDataSizeInBytes;

        SizeAwareStringList(final int capacity) {
            m_data = new String[capacity];
            // TODO(AP-23862) consider the object header size of array class?
            m_accumulatedDataSizeInBytes = new LargeOffsetBuffer(capacity);
        }

        void set(final int index, final String val) {
            m_data[index] = val;
            m_accumulatedDataSizeInBytes.add(index, StringSizeUtils.memorySize(val));
        }

        int capacity() {
            return m_data.length;
        }

        long usedSizeFor(final int numElements) {
            return m_accumulatedDataSizeInBytes.getNumData(numElements);
        }

        /** @return the consumed memory of this object in bytes */
        long sizeOf() {
            return usedSizeFor(capacity()) // m_data
                + LargeOffsetBuffer.usedSizeFor(capacity()); // m_accumulatedEncodedSizeInBytes
        }

        public void setNumElements(final int numElements) {
            m_data = Arrays.copyOf(m_data, numElements);
            m_accumulatedDataSizeInBytes.setNumElements(numElements);
        }
    }

    /** Arrow implementation of {@link StringReadData}. */
    public static final class ArrowStringWriteData
        extends AbstractOnHeapArrowWriteData<SizeAwareStringList, ArrowStringReadData> implements StringWriteData {

        private ArrowStringWriteData(final int capacity) {
            super(new SizeAwareStringList(capacity), capacity);
        }

        private ArrowStringWriteData(final SizeAwareStringList data, final ValidityBuffer validity, final int offset) {
            super(data, validity, offset, data.capacity());
        }

        @Override
        public void setString(final int index, final String val) {
            m_data.set(index + m_offset, val);
            setValid(index + m_offset);
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return new ArrowStringWriteData(m_data, m_validity, m_offset + start);
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return m_data.usedSizeFor(numElements) // m_data String[]
                + ValidityBuffer.usedSizeFor(numElements); // validity buffer
        }

        @Override
        public long sizeOf() {
            return m_data.sizeOf() + m_validity.sizeOf();
        }

        @Override
        protected ArrowStringReadData createReadData(final int length) {
            return new ArrowStringReadData(m_data.usedSizeFor(length), m_data.m_data, m_validity);
        }

        @Override
        protected void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_data.setNumElements(numElements);
        }
    }

    /** Arrow implementation of {@link StringReadData}. */
    public static final class ArrowStringReadData extends AbstractOnHeapArrowReadData<String[]>
        implements StringReadData {

        private final long m_dataSizeInBytes;

        private ArrowStringReadData(final long dataSizeInBytes, final String[] data, final ValidityBuffer validity) {
            super(data, validity, data.length);
            m_dataSizeInBytes = dataSizeInBytes;
        }

        private ArrowStringReadData(final long dataSizeInBytes, final String[] data, final ValidityBuffer validity,
            final int offset, final int length) {
            super(data, validity, offset, length);
            m_dataSizeInBytes = dataSizeInBytes;
        }

        @Override
        public String getString(final int index) {
            return m_data[index + m_offset];
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return new ArrowStringReadData(m_dataSizeInBytes, m_data, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return m_dataSizeInBytes + m_validity.sizeOf();
        }
    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowStringData} */
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

            vector.allocateNew(d.length());

            // Copy the data
            for (int i = 0; i < d.length(); i++) {
                if (!d.isMissing(i)) {
                    var val = d.getString(i);
                    if (val != null) {
                        // TODO(AP-23858) Is CharsetEncoder faster?
                        // TODO(AP-23858) Can encode directly to the ArrowBuf without a temporary byte[]?
                        vector.setSafe(i, val.getBytes(StandardCharsets.UTF_8));
                    } else {
                        vector.setNull(i);
                    }
                } else {
                    vector.setNull(i);
                }
            }

            // Copy the validity
            d.m_validity.copyTo(vector.getValidityBuffer());

            vector.setValueCount(d.length());
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public ArrowStringReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {

                var valueCount = vector.getValueCount();
                var v = (VarCharVector)vector;

                long dataSizeInBytes = 0;
                var data = new String[valueCount];

                for (int i = 0; i < valueCount; i++) {
                    if (!v.isNull(i)) {
                        var valueBytes = v.get(i);
                        if (valueBytes != null) {
                            // TODO(AP-23858) Is CharsetDecoder faster?
                            // TODO(AP-23858) Can decode directly from the ArrowBuf without a temporary byte[]?
                            data[i] = new String(valueBytes, StandardCharsets.UTF_8);
                            dataSizeInBytes += data[i].length() * 2L;
                        }
                    }
                }
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                return new ArrowStringReadData(dataSizeInBytes, data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowStringData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return Long.BYTES // Long offsets for counting the accumulated size
                + Integer.BYTES; // Compressed OOPs in the data String[]
        }
    }
}
