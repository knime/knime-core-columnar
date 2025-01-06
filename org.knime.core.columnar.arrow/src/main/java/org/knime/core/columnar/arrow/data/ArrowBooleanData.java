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

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.NullableReadData;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class ArrowBooleanData {

    private ArrowBooleanData() {
    }

    public static final class ArrowBooleanWriteData extends AbstractArrowWriteData implements BooleanWriteData {

        private int m_capacity;

        // TODO rename ValidityBuffer to bitvector or so
        private ValidityBuffer m_data;

        private ArrowBooleanWriteData(final int capacity) {
            super(capacity);
            m_data = new ValidityBuffer(capacity);
            m_capacity = capacity;
        }

        private ArrowBooleanWriteData(final int offset, final ValidityBuffer data, final ValidityBuffer validity,
            final int capacity) {
            super(offset, validity);
            m_data = data;
            m_capacity = capacity;
        }

        @Override
        public void expand(final int minimumCapacity) {
            setNumElements(minimumCapacity);
            m_capacity = minimumCapacity;
        }

        @Override
        public int capacity() {
            return m_capacity;
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
        public ArrowWriteData slice(final int start) {
            return new ArrowBooleanWriteData(m_offset + start, m_data, m_validity, m_capacity);
        }

        @Override
        public void setBoolean(final int index, final boolean val) {
            m_data.set(index + m_offset, val);
            setValid(index + m_offset);
        }

        @Override
        protected void closeResources() {
            // No resources to close for on-heap data
        }

        @Override
        public ArrowBooleanReadData close(final int length) {
            setNumElements(length);
            return new ArrowBooleanReadData(m_data, m_validity, length);
        }

        /**
         * Expand or shrink the data to the given size.
         *
         * @param numElements the new size of the data
         */
        private void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_data.setNumElements(numElements);
        }
    }

    public static final class ArrowBooleanReadData extends AbstractArrowReadData implements BooleanReadData {

        private final ValidityBuffer m_data;

        public ArrowBooleanReadData(final ValidityBuffer data, final ValidityBuffer validity, final int length) {
            super(validity, length);
            m_data = data;
        }

        public ArrowBooleanReadData(final ValidityBuffer data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(validity, offset, length);
            m_data = data;
        }

        @Override
        public long sizeOf() {
            return ValidityBuffer.usedSizeFor(m_length) * 2;
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowBooleanReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public boolean getBoolean(final int index) {
            return m_data.isSet(index + m_offset);
        }
    }

    public static final class ArrowBooleanDataFactory extends AbstractArrowColumnDataFactory {

        public static final ArrowBooleanDataFactory INSTANCE = new ArrowBooleanDataFactory();

        private ArrowBooleanDataFactory() {
            super(0);
        }

        @Override
        public ArrowBooleanReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            if (m_version.equals(version)) {
                var valueCount = vector.getValueCount();
                var data = ValidityBuffer.createFrom(vector.getDataBuffer(), valueCount);
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);
                return new ArrowBooleanReadData(data, validity, valueCount);
            } else {
                throw new IOException(
                    "Cannot read ArrowIntData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public ArrowBooleanWriteData createWrite(final int capacity) {
            return new ArrowBooleanWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.BIT.getType());
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector fieldVector) {
            var d = (ArrowBooleanReadData)data; // TODO generic?
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
        public int initialNumBytesPerElement() {
            return 1;
        }
    }
}
