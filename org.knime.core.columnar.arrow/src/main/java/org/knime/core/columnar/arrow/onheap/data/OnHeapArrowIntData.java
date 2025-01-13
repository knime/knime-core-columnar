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
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Arrow implementation of {@link IntWriteData} and {@link IntReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowIntData {

    private OnHeapArrowIntData() {
    }

    /** Arrow implementation of {@link IntWriteData}. */
    public static final class ArrowIntWriteData extends AbstractOnHeapArrowWriteData<int[]> implements IntWriteData {

        private ArrowIntWriteData(final int capacity) {
            super(new int[capacity], capacity);
        }

        private ArrowIntWriteData(final int[] data, final ValidityBuffer validity, final int offset) {
            super(data, validity, offset);
        }

        @Override
        public void setInt(final int index, final int val) {
            m_data[index + m_offset] = val;
            setValid(index + m_offset);
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return new ArrowIntWriteData(m_data, m_validity, m_offset + start);
        }

        @Override
        public void expand(final int minimumCapacity) {
            setNumElements(minimumCapacity);
        }

        @Override
        public int capacity() {
            return m_data.length;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements * Integer.BYTES + ValidityBuffer.usedSizeFor(numElements);
        }

        @Override
        public long sizeOf() {
            return m_data.length * Integer.BYTES + m_validity.sizeOf();
        }

        @Override
        public ArrowIntReadData close(final int length) {
            setNumElements(length);
            var readData = new ArrowIntReadData(m_data, m_validity);
            closeResources();
            return readData;
        }

        /**
         * Expand or shrink the data to the given size.
         *
         * @param numElements the new size of the data
         */
        private void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);

            var newData = new int[numElements];
            System.arraycopy(m_data, 0, newData, 0, Math.min(m_data.length, numElements));
            m_data = newData;
        }
    }

    /** Arrow implementation of {@link IntReadData}. */
    public static final class ArrowIntReadData extends AbstractOnHeapArrowReadData<int[]> implements IntReadData {

        private ArrowIntReadData(final int[] data, final ValidityBuffer validity) {
            super(data, validity, data.length);
        }

        private ArrowIntReadData(final int[] data, final ValidityBuffer validity, final int offset, final int length) {
            super(data, validity, offset, length);
        }

        @Override
        public int getInt(final int index) {
            return m_data[index + m_offset];
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return new ArrowIntReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return m_data.length * Integer.BYTES + m_validity.sizeOf();
        }
    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowIntData} */
    public static final class ArrowIntDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowIntDataFactory} */
        public static final ArrowIntDataFactory INSTANCE = new ArrowIntDataFactory(true);

        /** Singleton instance of {@link ArrowIntDataFactory} using an unsigned vector */
        public static final ArrowIntDataFactory INSTANCE_UNSIGNED = new ArrowIntDataFactory(false);

        private final boolean m_signed;

        private ArrowIntDataFactory(final boolean signed) {
            super(0);
            m_signed = signed;
        }

        @Override
        public ArrowIntWriteData createWrite(final int capacity) {
            return new ArrowIntWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            if (m_signed) {
                return Field.nullable(name, MinorType.INT.getType());
            } else {
                // Note we just use an unsigned type for the vector but the rest of the code is the same
                // because Java has no unsigned integers
                return Field.nullable(name, MinorType.UINT4.getType());
            }
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            var d = (ArrowIntReadData)data;

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
        public ArrowIntReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            if (m_version.equals(version)) {
                var valueCount = vector.getValueCount();
                var data = new int[valueCount];
                MemoryCopyUtils.copy(vector.getDataBufferAddress(), data);
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                return new ArrowIntReadData(data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowIntData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return Integer.BYTES + 1; // +1 for validity
        }
    }
}
