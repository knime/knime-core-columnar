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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Arrow implementation of {@link DoubleWriteData} and {@link DoubleReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowDoubleData {

    private OnHeapArrowDoubleData() {
    }

    /** Arrow implementation of {@link DoubleWriteData}. */
    public static final class ArrowDoubleWriteData extends AbstractOnHeapArrowWriteData<double[], ArrowDoubleReadData>
        implements DoubleWriteData {

        private ArrowDoubleWriteData(final int capacity) {
            super(new double[capacity], capacity);
        }

        private ArrowDoubleWriteData(final double[] data, final ValidityBuffer validity, final int offset) {
            super(data, validity, offset, data.length);
        }

        @Override
        public void setDouble(final int index, final double val) {
            m_data[index + m_offset] = val;
            setValid(index);
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return new ArrowDoubleWriteData(m_data, m_validity, m_offset + start);
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
        protected ArrowDoubleReadData createReadData(final int length) {
            return new ArrowDoubleReadData(m_data, m_validity);
        }

        @Override
        protected void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_data = Arrays.copyOf(m_data, numElements);
        }
    }

    /** Arrow implementation of {@link DoubleReadData}. */
    public static final class ArrowDoubleReadData extends AbstractOnHeapArrowReadData<double[]>
        implements DoubleReadData {

        private ArrowDoubleReadData(final double[] data, final ValidityBuffer validity) {
            super(data, validity, data.length);
        }

        private ArrowDoubleReadData(final double[] data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(data, validity, offset, length);
        }

        @Override
        public double getDouble(final int index) {
            return m_data[index + m_offset];
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return new ArrowDoubleReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return m_data.length * Double.BYTES + m_validity.sizeOf();
        }
    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowDoubleData} */
    public static final class ArrowDoubleDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowDoubleDataFactory} */
        public static final ArrowDoubleDataFactory INSTANCE = new ArrowDoubleDataFactory();

        private ArrowDoubleDataFactory() {
            super(0);
        }

        @Override
        public ArrowDoubleWriteData createWrite(final int capacity) {
            return new ArrowDoubleWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.FLOAT8.getType());
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public void copyToVector(final NullableReadData data, final FieldVector fieldVector) {
            var d = (ArrowDoubleReadData)data;
            d.checkNotSliced();
            var vector = (Float8Vector)fieldVector;

            vector.allocateNew(d.length());

            // Copy the data
            MemoryCopyUtils.copy(d.m_data, vector.getDataBuffer());

            // Copy the validity
            d.m_validity.copyTo(vector.getValidityBuffer());

            // Set the value count
            vector.setValueCount(d.length());
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public ArrowDoubleReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            if (m_version.equals(version)) {
                var valueCount = vector.getValueCount();
                var data = new double[valueCount];
                MemoryCopyUtils.copy(vector.getDataBuffer(), data);
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                return new ArrowDoubleReadData(data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowDoubleData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return Double.BYTES + 1; // +1 for validity
        }
    }
}
