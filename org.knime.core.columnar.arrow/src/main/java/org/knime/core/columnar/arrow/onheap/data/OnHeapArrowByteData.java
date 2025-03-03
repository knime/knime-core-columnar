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
import java.util.Arrays;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Arrow implementation of {@link ByteWriteData} and {@link ByteReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowByteData {

    private OnHeapArrowByteData() {
    }

    /** Arrow implementation of {@link ByteWriteData}. */
    public static final class ArrowByteWriteData extends AbstractOnHeapArrowWriteData<byte[], ArrowByteReadData>
        implements ByteWriteData {

        private ArrowByteWriteData(final int capacity) {
            super(new byte[capacity], capacity);
        }

        private ArrowByteWriteData(final byte[] data, final ValidityBuffer validity, final int offset) {
            super(data, validity, offset, data.length);
        }

        @Override
        public void setByte(final int index, final byte val) {
            m_data[index + m_offset] = val;
            setValid(index + m_offset);
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return new ArrowByteWriteData(m_data, m_validity, m_offset + start);
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements + ValidityBuffer.usedSizeFor(numElements);
        }

        @Override
        public long sizeOf() {
            return m_data.length + m_validity.sizeOf();
        }

        @Override
        protected ArrowByteReadData createReadData(final int length) {
            return new ArrowByteReadData(m_data, m_validity);
        }

        @Override
        protected void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_data = Arrays.copyOf(m_data, numElements);
        }
    }

    /** Arrow implementation of {@link ByteReadData}. */
    public static final class ArrowByteReadData extends AbstractOnHeapArrowReadData<byte[]> implements ByteReadData {

        private ArrowByteReadData(final byte[] data, final ValidityBuffer validity) {
            super(data, validity, data.length);
        }

        private ArrowByteReadData(final byte[] data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(data, validity, offset, length);
        }

        @Override
        public byte getByte(final int index) {
            return m_data[index + m_offset];
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return new ArrowByteReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return m_data.length + m_validity.sizeOf();
        }
    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowByteData} */
    public static final class ArrowByteDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowByteDataFactory} */
        public static final ArrowByteDataFactory INSTANCE = new ArrowByteDataFactory(true);

        /** Singleton instance of {@link ArrowByteDataFactory} using an unsigned arrow vector */
        public static final ArrowByteDataFactory INSTANCE_UNSIGNED = new ArrowByteDataFactory(false);

        private final boolean m_signed;

        private ArrowByteDataFactory(final boolean signed) {
            super(0);
            m_signed = signed;
        }

        @Override
        public ArrowByteWriteData createWrite(final int capacity) {
            return new ArrowByteWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            if (m_signed) {
                return Field.nullable(name, MinorType.TINYINT.getType());
            } else {
                // Note we just use an unsigned type for the vector but the rest of the code is the same
                // because Java has no unsigned bytes
                return Field.nullable(name, MinorType.UINT1.getType());
            }
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            var d = (ArrowByteReadData)data;
            d.checkNotSliced();

            ((FixedWidthVector)vector).allocateNew(d.length());

            // Copy the data
            vector.getDataBuffer().setBytes(0, d.m_data);

            // Copy the validity
            d.m_validity.copyTo(vector.getValidityBuffer());

            // Set the value count
            vector.setValueCount(d.length());
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public ArrowByteReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            if (m_version.equals(version)) {
                var valueCount = vector.getValueCount();
                var data = new byte[valueCount];
                vector.getDataBuffer().getBytes(0, data);
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                return new ArrowByteReadData(data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowByteData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return Byte.BYTES + 1; // +1 for validity
        }
    }
}
