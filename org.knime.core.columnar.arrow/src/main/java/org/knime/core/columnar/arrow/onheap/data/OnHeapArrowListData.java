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
 *   Oct 13, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import java.io.IOException;
import java.util.Collections;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Arrow implementation of {@link ListWriteData} and {@link ListReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowListData {

    private OnHeapArrowListData() {
    }

    /** Arrow implementation of {@link ListWriteData}. */
    public static final class ArrowListWriteData
        extends AbstractOnHeapArrowWriteData<OnHeapArrowWriteData, ArrowListReadData> implements ListWriteData {

        private OffsetBuffer m_offsets;

        private ArrowListWriteData(final int capacity, final OnHeapArrowWriteData data) {
            super(data, capacity);
            m_capacity = capacity;
            m_offsets = new OffsetBuffer(capacity);
        }

        private ArrowListWriteData(final OnHeapArrowWriteData data, final OffsetBuffer offsets,
            final ValidityBuffer validity, final int offset, final int capacity) {
            super(data, validity, offset, capacity);
            m_offsets = offsets;
        }

        @Override
        public <C extends NullableWriteData> C createWriteData(final int index, final int size) {
            var dataIndex = m_offsets.add(index + m_offset, size);

            // Set the validity bit
            setValid(index + m_offset);

            // TODO(AP-23863) be smarter here and do not copy on each new list
            m_data.expand(dataIndex.end());

            @SuppressWarnings("unchecked")
            var data = (C)m_data.slice(dataIndex.start());
            return data;
        }

        @Override
        public OnHeapArrowWriteData slice(final int start) {
            return new ArrowListWriteData(m_data, m_offsets, m_validity, m_offset + start, m_capacity);
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return ValidityBuffer.usedSizeFor(numElements) //
                + OffsetBuffer.usedSizeFor(numElements) //
                + m_data.usedSizeFor(m_offsets.getNumData(numElements));
        }

        @Override
        public long sizeOf() {
            return m_validity.sizeOf() + m_offsets.sizeOf() + m_data.sizeOf();
        }

        @Override
        protected ArrowListReadData createReadData(final int length) {
            m_offsets.fillWithZeroLength();
            return new ArrowListReadData(m_data.close(m_offsets.getNumData(length)), m_offsets, m_validity, length);
        }

        @Override
        protected void closeResources() {
            if (m_data != null) {
                m_data.release();
            }
            super.closeResources();
            m_offsets = null;
        }

        @Override
        protected void setNumElements(final int numElements) {
            // Note: m_data is expanded when setting the elements inside the list
            m_validity.setNumElements(numElements);
            m_offsets.setNumElements(numElements);
        }
    }

    /** Arrow implementation of {@link ListReadData}. */
    public static final class ArrowListReadData extends AbstractOnHeapArrowReadData<OnHeapArrowReadData>
        implements ListReadData {

        private OffsetBuffer m_offsets;

        private ArrowListReadData(final OnHeapArrowReadData data, final OffsetBuffer offsets,
            final ValidityBuffer validity, final int length) {
            super(data, validity, length);
            m_offsets = offsets;
        }

        private ArrowListReadData(final OnHeapArrowReadData data, final OffsetBuffer offsets,
            final ValidityBuffer validity, final int offset, final int length) {
            super(data, validity, offset, length);
            m_offsets = offsets;
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
        public OnHeapArrowReadData slice(final int start, final int length) {
            return new ArrowListReadData(m_data, m_offsets, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return m_validity.sizeOf() + m_offsets.sizeOf() + m_data.sizeOf();
        }

        @Override
        protected void closeResources() {
            if (m_data != null) {
                m_data.release();
            }
            super.closeResources();
            m_offsets = null;
        }
    }

    /**
     * Implementation of {@link OnHeapArrowColumnDataFactory} for {@link ArrowListReadData} and
     * {@link ArrowListWriteData}.
     */
    public static final class ArrowListDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private static final int INITIAL_VALUES_PER_LIST = 10;

        /**
         * Create a new factory for Arrow list data.
         *
         * @param inner the factory to create the type of the list elements
         */
        public ArrowListDataFactory(final OnHeapArrowColumnDataFactory inner) {
            super(CURRENT_VERSION, inner);
        }

        @Override
        public OnHeapArrowWriteData createWrite(final int capacity) {
            return new ArrowListWriteData(capacity, m_children[0].createWrite(capacity * INITIAL_VALUES_PER_LIST));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final Field data = m_children[0].getField("listData", dictionaryIdSupplier);
            return new Field(name, new FieldType(true, MinorType.LIST.getType(), null),
                Collections.singletonList(data));
        }

        @Override
        @SuppressWarnings("resource") // buffers and child vectors are owned by the vector
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            var d = (ArrowListReadData)data;
            d.checkNotSliced();
            var v = (ListVector)vector;

            // Note: we must do that before creating the inner data because "allocateNew" overwrites the allocation for
            // the child vector
            v.setInitialCapacity(d.length());
            v.allocateNew();

            m_children[0].copyToVector(d.m_data, v.getDataVector());

            d.m_validity.copyTo(vector.getValidityBuffer());
            d.m_offsets.copyTo(vector.getOffsetBuffer());

            v.setLastSet(d.length() - 1);
            v.setValueCount(d.length());
        }

        @Override
        @SuppressWarnings("resource") // buffers and child vectors are owned by the vector
        public OnHeapArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (version.getVersion() == CURRENT_VERSION) {
                var valueCount = vector.getValueCount();
                var listVector = (ListVector)vector;
                var dataVector = listVector.getDataVector();
                var offsets = OffsetBuffer.createFrom(vector.getOffsetBuffer(), valueCount);
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                var data =
                    m_children[0].createRead(dataVector, nullCount.getChild(0), provider, version.getChildVersion(0));
                return new ArrowListReadData(data, offsets, validity, valueCount);
            } else {
                throw new IOException(
                    "Cannot read ArrowListData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return m_children[0].initialNumBytesPerElement() * INITIAL_VALUES_PER_LIST // data buffer
                + Integer.BYTES // offset buffer
                + 1; // validity bit
        }
    }
}
