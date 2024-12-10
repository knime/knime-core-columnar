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
 *   Sep 20, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.NullableReadData;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OnHeapDoubleData {

    public static final Factory FACTORY = new Factory();

    private OnHeapDoubleData() {
    }

    public static final class OnHeapDoubleWriteData extends AbstractArrowWriteData implements DoubleWriteData {

        private double[] m_data;

        private OnHeapDoubleWriteData(final int capacity) {
            super(capacity);
            m_data = new double[capacity];
        }

        private OnHeapDoubleWriteData(final int offset, final double[] data, final ValidityBuffer validity) {
            super(offset, validity);
            m_data = data;
        }

        @Override
        public void expand(final int minimumCapacity) {
            setNumElements(minimumCapacity);
        }

        @Override
        public int capacity() {
            return m_data.length; // TODO - offset??
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements * Double.BYTES + ValidityBuffer.usedSizeFor(numElements);
        }

        @Override
        public long sizeOf() {
            return m_data.length * Double.BYTES + m_validity.sizeOf();
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new OnHeapDoubleWriteData(m_offset + start, m_data, m_validity);
        }

        @Override
        public void setDouble(final int index, final double val) {
            m_data[index + m_offset] = val;
            setValid(index + m_offset);
        }

        @Override
        protected void closeResources() {
            // TODO not needed for on-heap, right?
        }

        @Override
        public OnHeapDoubleReadData close(final int length) {
            setNumElements(length);
            return new OnHeapDoubleReadData(m_data, m_validity);
        }

        /**
         * Expand or shrink the data to the given size.
         *
         * @param numElements the new size of the data
         */
        private void setNumElements(final int numElements) {
            // TODO we copy each time. Only copy if necessary! (implement for all data types)
            m_validity.setNumElements(numElements);

            var newData = new double[numElements];
            System.arraycopy(m_data, 0, newData, 0, Math.min(m_data.length, numElements));
            m_data = newData;
        }
    }

    public static final class OnHeapDoubleReadData extends AbstractArrowReadData implements DoubleReadData {

        private final double[] m_data;

        public OnHeapDoubleReadData(final double[] data, final ValidityBuffer validity) {
            super(validity, data.length);
            m_data = data;
        }

        public OnHeapDoubleReadData(final double[] data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(validity, offset, length);
            m_data = data;
        }

        @Override
        public long sizeOf() {
            return m_data.length * Double.BYTES + m_validity.sizeOf();
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new OnHeapDoubleReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public double getDouble(final int index) {
            return m_data[index + m_offset];
        }

    }

    // TODO extract common functionality
    public static final class Factory extends AbstractArrowColumnDataFactory {

        private Factory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        private static final int DOUBLE_ARRAY_BASE_OFFSET = MemoryUtil.UNSAFE.arrayBaseOffset(double[].class);

        @Override
        public OnHeapDoubleReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            // TODO what is the null count for???

            if (m_version.equals(version)) {
                var valueCount = vector.getValueCount();
                var data = new double[valueCount];
                MemoryUtil.UNSAFE.copyMemory(//
                    null, //
                    vector.getDataBufferAddress(), //
                    data, //
                    DOUBLE_ARRAY_BASE_OFFSET, //
                    data.length * Double.BYTES //
                );
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                return new OnHeapDoubleReadData(data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowDoubleData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public OnHeapDoubleWriteData createWrite(final int capacity) {
            return new OnHeapDoubleWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.FLOAT8.getType());
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector fieldVector) {
            var d = (OnHeapDoubleReadData)data; // TODO generic?
            var vector = (Float8Vector)fieldVector;

            vector.allocateNew(d.length());

            // Copy the data
            // Or should we rather use ByteBuffer for `m_data` and use vector.getDataBuffer().setBytes()
            MemoryUtil.UNSAFE.copyMemory(//
                d.m_data, //
                DOUBLE_ARRAY_BASE_OFFSET, //
                null, //
                vector.getDataBufferAddress(), //
                d.m_data.length * vector.getTypeWidth() //
            );

            // Copy the validity
            d.m_validity.copyTo(vector.getValidityBuffer());

            // Set the value count
            vector.setValueCount(d.length());
        }

        @Override
        public int initialNumBytesPerElement() {
            // TODO
            return 9;
        }
    }
}
