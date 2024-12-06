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
 *   Dec 3, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.Collections;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.OffsetsBuffer.IntOffsetsReadBuffer;
import org.knime.core.columnar.arrow.data.OffsetsBuffer.IntOffsetsWriteBuffer;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OnHeapListData {

    private OnHeapListData() {
    }

    public static final class OnHeapListWriteData extends AbstractArrowWriteData implements ListWriteData {

        private int m_capacity;

        private ArrowWriteData m_data;

        private IntOffsetsWriteBuffer m_offsets;

        private OnHeapListWriteData(final int capacity, final ArrowWriteData data) {
            super(capacity);
            m_capacity = capacity;
            m_data = data;
            m_offsets = OffsetsBuffer.createIntWriteBuffer(capacity);
        }

        private OnHeapListWriteData(final int offset, final ArrowWriteData data, final IntOffsetsWriteBuffer offsets,
            final ValidityBuffer validity, final int capacity) {
            super(offset, validity);
            m_capacity = capacity;
            m_data = data;
            m_offsets = offsets;
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
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long sizeOf() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public OnHeapListReadData close(final int length) {
            // NOTE: setNumElements also shrinks the offsets buffer if necessary
            setNumElements(length);
            var readOffsets = m_offsets.close();
            return new OnHeapListReadData(m_data.close(readOffsets.get(length - 1).end()), readOffsets, m_validity,
                length);
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new OnHeapListWriteData(m_offset + start, m_data, m_offsets, m_validity, m_capacity);
        }

        @Override
        public <C extends NullableWriteData> C createWriteData(final int index, final int size) {
            var dataIndex = m_offsets.add(index + m_offset, size);

            // Set the validity bit
            setValid(index + m_offset);

            // TODO be smarter here and do not copy on each new list
            m_data.expand(dataIndex.end());

            @SuppressWarnings("unchecked")
            var data = (C)m_data.slice(dataIndex.start());
            return data;
        }

        @Override
        protected void closeResources() {
            // TODO not needed for on-heap, right?
        }

        /**
         * Expand or shrink the data to the given size.
         *
         * @param numElements the new size of the data
         */
        private void setNumElements(final int numElements) {
            // Note: m_data is expanded when setting the elements inside the list
            m_validity.setNumElements(numElements);
            m_offsets.setNumElements(numElements);
        }
    }

    public static final class OnHeapListReadData extends AbstractArrowReadData implements ListReadData {

        private final ArrowReadData m_data;

        private final IntOffsetsReadBuffer m_offsets;

        private OnHeapListReadData(final ArrowReadData data, final IntOffsetsReadBuffer readOffsets,
            final ValidityBuffer validity, final int length) {
            super(validity, length);
            m_data = data;
            m_offsets = readOffsets;
        }

        private OnHeapListReadData(final ArrowReadData data, final IntOffsetsReadBuffer offsets,
            final ValidityBuffer validity, final int offset, final int length) {
            super(validity, offset, length);
            m_data = data;
            m_offsets = offsets;
        }

        @Override
        public long sizeOf() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public <C extends NullableReadData> C createReadData(final int index) {
            // Slice the data
            var dataIndex = m_offsets.get(index + m_offset);
            @SuppressWarnings("unchecked")
            final C data = (C)m_data.slice(dataIndex.start(), dataIndex.end() - dataIndex.start());
            return data;
        }

        @Override
        protected void closeResources() {
            // TODO not needed for on-heap, right?
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new OnHeapListReadData(m_data, m_offsets, m_validity, m_offset + start, length);
        }
    }

    public static final class Factory extends AbstractArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private final ArrowColumnDataFactory m_inner;

        protected Factory(final ArrowColumnDataFactory inner) {
            super(ArrowColumnDataFactoryVersion.version(CURRENT_VERSION));
            m_inner = inner;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final Field data = m_inner.getField("listData", dictionaryIdSupplier);
            return new Field(name, new FieldType(true, MinorType.LIST.getType(), null),
                Collections.singletonList(data));
        }

        @Override
        public ArrowWriteData createWrite(final int capacity) {
            return new OnHeapListWriteData(capacity, m_inner.createWrite(capacity));
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                var valueCount = vector.getValueCount();
                var listVector = (ListVector)vector;
                var dataVector = listVector.getDataVector();
                var offsets = OffsetsBuffer.createIntReadBuffer(vector.getOffsetBuffer(), valueCount);
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                var data = m_inner.createRead(dataVector, nullCount, provider, version);
                return new OnHeapListReadData(data, offsets, validity, valueCount);
            } else {
                throw new IOException(
                    "Cannot read ArrowListData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            var d = (OnHeapListReadData)data;
            var v = (ListVector)vector;

            // TODO is this comment still valid?
            // Note: we must do that before creating the inner data because "allocateNew" overwrites the allocation for
            // the child vector
            v.setInitialCapacity(d.length());
            v.allocateNew();

            m_inner.copyToVector(d.m_data, v.getDataVector());

            d.m_validity.copyTo(vector.getValidityBuffer());
            d.m_offsets.copyTo(vector.getOffsetBuffer());

            v.setLastSet(d.length() - 1);
            v.setValueCount(d.length());
        }

        @Override
        public int initialNumBytesPerElement() {
            // TODO Auto-generated method stub
            return 0;
        }
    }
}
