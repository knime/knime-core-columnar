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
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Arrow implementation of {@link LongWriteData} and {@link LongReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowLongData {

    private OnHeapArrowLongData() {
    }

    /** Arrow implementation of {@link LongWriteData}. */
    public static final class ArrowLongWriteData extends AbstractOnHeapArrowWriteData<long[], ArrowLongReadData>
        implements LongWriteData {

        private ArrowLongWriteData(final int capacity) {
            super(new long[capacity], capacity);
        }

        private ArrowLongWriteData(final long[] data, final ValidityBuffer validity, final int offset) {
            super(data, validity, offset, data.length);
        }

        @Override
        public void setLong(final int index, final long val) {
            m_data[index + m_offset] = val;
            setValid(index);
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return new ArrowLongWriteData(m_data, m_validity, m_offset + start);
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements * Long.BYTES + ValidityBuffer.usedSizeFor(numElements);
        }

        @Override
        public long sizeOf() {
            return m_data.length * Long.BYTES + m_validity.sizeOf();
        }

        @Override
        protected ArrowLongReadData createReadData(final int length) {
            return new ArrowLongReadData(m_data, m_validity);
        }

        @Override
        protected void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_data = Arrays.copyOf(m_data, numElements);
        }
    }

    /** Arrow implementation of {@link LongReadData}. */
    public static final class ArrowLongReadData extends AbstractOnHeapArrowReadData<long[]> implements LongReadData {

        private ArrowLongReadData(final long[] data, final ValidityBuffer validity) {
            super(data, validity, data.length);
        }

        private ArrowLongReadData(final long[] data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(data, validity, offset, length);
        }

        @Override
        public long getLong(final int index) {
            return m_data[index + m_offset];
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return new ArrowLongReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return m_data.length * Long.BYTES + m_validity.sizeOf();
        }
    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowLongData} */
    public static final class ArrowLongDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowLongDataFactory} */
        public static final ArrowLongDataFactory INSTANCE = new ArrowLongDataFactory(true);

        /** Singleton instance of {@link ArrowLongDataFactory} using an unsigned vector */
        public static final ArrowLongDataFactory INSTANCE_UNSIGNED = new ArrowLongDataFactory(false);

        /**
         * Also supports reading longs from DurationVectors and TimeNanoVectors. This is necessary for backwards
         * compatibility of date&time types which now simply use BigIntVector.
         */
        private static final ArrowColumnDataFactoryVersion V0 = ArrowColumnDataFactoryVersion.version(0);

        private final boolean m_signed;

        private ArrowLongDataFactory(final boolean signed) {
            super(1);
            m_signed = signed;
        }

        @Override
        public ArrowLongWriteData createWrite(final int capacity) {
            return new ArrowLongWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            if (m_signed) {
                return Field.nullable(name, MinorType.BIGINT.getType());
            } else {
                // Note we just use an unsigned type for the vector but the rest of the code is the same
                // because Java has no unsigned longs
                return Field.nullable(name, MinorType.UINT8.getType());
            }
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            var d = (ArrowLongReadData)data;
            d.checkNotSliced();

            ((FixedWidthVector)vector).allocateNew(d.length());

            // Copy the data
            MemoryCopyUtils.copy(d.m_data, vector.getDataBufferAddress());

            // Copy the validity
            d.m_validity.copyTo(vector.getValidityBuffer());

            // Set the value count
            vector.setValueCount(d.length());
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public ArrowLongReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            // NOTE:
            // V1 was introduced for backward compatibility with old date&time types. Previously, (V0) we had separate
            // data implementations for them that used DurationVector and TimeNanoVector. For backward compatibility, we
            // read these vectors into LongData as well. They require no special handling here because they are just
            // signed 64-bit integers.
            if (V0.equals(version) || m_version.equals(version)) {
                var valueCount = vector.getValueCount();
                var data = new long[valueCount];
                MemoryCopyUtils.copy(vector.getDataBufferAddress(), data);
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                return new ArrowLongReadData(data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowLongData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return Long.BYTES + 1; // +1 for validity
        }
    }
}
