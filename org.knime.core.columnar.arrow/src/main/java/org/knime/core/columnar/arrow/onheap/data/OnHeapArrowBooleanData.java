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

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Arrow implementation of {@link BooleanWriteData} and {@link BooleanReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowBooleanData {

    private OnHeapArrowBooleanData() {
    }

    /** Arrow implementation of {@link BooleanWriteData}. */
    public static final class ArrowBooleanWriteData
        extends AbstractOnHeapArrowWriteData<ValidityBuffer, ArrowBooleanReadData> implements BooleanWriteData {

        private ArrowBooleanWriteData(final int capacity) {
            super(new ValidityBuffer(capacity), capacity);
        }

        private ArrowBooleanWriteData(final ValidityBuffer data, final ValidityBuffer validity, final int offset,
            final int capacity) {
            super(data, validity, offset, capacity);
        }

        @Override
        public void setBoolean(final int index, final boolean val) {
            m_data.set(index + m_offset, val);
            setValid(index);
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return new ArrowBooleanWriteData(m_data, m_validity, m_offset + start, m_capacity);
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return ValidityBuffer.usedSizeFor(numElements) * 2;
        }

        @Override
        public long sizeOf() {
            return usedSizeFor(m_capacity);
        }

        @Override
        protected ArrowBooleanReadData createReadData(final int length) {
            return new ArrowBooleanReadData(m_data, m_validity, length);
        }

        @Override
        protected void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_data.setNumElements(numElements);
        }
    }

    /** Arrow implementation of {@link BooleanReadData}. */
    public static final class ArrowBooleanReadData extends AbstractOnHeapArrowReadData<ValidityBuffer>
        implements BooleanReadData {

        private ArrowBooleanReadData(final ValidityBuffer data, final ValidityBuffer validity, final int length) {
            super(data, validity, length);
        }

        private ArrowBooleanReadData(final ValidityBuffer data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(data, validity, offset, length);
        }

        @Override
        public boolean getBoolean(final int index) {
            return m_data.isSet(index + m_offset);
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            return new ArrowBooleanReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ValidityBuffer.usedSizeFor(m_length) * 2;
        }
    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowBooleanData} */
    public static final class ArrowBooleanDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowBooleanDataFactory} */
        public static final ArrowBooleanDataFactory INSTANCE = new ArrowBooleanDataFactory();

        private ArrowBooleanDataFactory() {
            super(0);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.BIT.getType());
        }

        @Override
        public ArrowBooleanWriteData createWrite(final int capacity) {
            return new ArrowBooleanWriteData(capacity);
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public void copyToVector(final NullableReadData data, final FieldVector fieldVector) {
            var d = (ArrowBooleanReadData)data;
            d.checkNotSliced();
            var vector = (BitVector)fieldVector;

            vector.allocateNew(d.length());

            // Copy the data
            d.m_data.copyTo(vector.getDataBuffer());

            // Copy the validity
            d.m_validity.copyTo(vector.getValidityBuffer());

            // Set the value count
            vector.setValueCount(d.length());
        }

        @Override
        @SuppressWarnings("resource") // buffers are owned by the vector
        public ArrowBooleanReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            if (m_version.equals(version)) {
                var valueCount = vector.getValueCount();
                var data = ValidityBuffer.createFrom(vector.getDataBuffer(), valueCount);
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);
                return new ArrowBooleanReadData(data, validity, valueCount);
            } else {
                throw new IOException(
                    "Cannot read ArrowBooleanData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return 1;
        }
    }
}
