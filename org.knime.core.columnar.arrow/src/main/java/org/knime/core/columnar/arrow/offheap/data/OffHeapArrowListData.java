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
package org.knime.core.columnar.arrow.offheap.data;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.offheap.OffHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.data.AbstractOffHeapArrowReadData.MissingValues;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Arrow implementation of {@link ListWriteData} and {@link ListReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OffHeapArrowListData {

    private OffHeapArrowListData() {
    }

    /** Arrow implementation of {@link ListWriteData}. */
    public static final class ArrowListWriteData extends AbstractOffHeapArrowWriteData<ListVector> implements ListWriteData {

        // Package private for testing only
        final OffHeapArrowWriteData m_data;

        private ArrowListWriteData(final ListVector vector, final OffHeapArrowWriteData data) {
            super(vector);
            m_data = data;
        }

        private ArrowListWriteData(final ListVector vector, final OffHeapArrowWriteData data, final int offset) {
            super(vector, offset);
            m_data = data;
        }

        @Override
        public <C extends NullableWriteData> C createWriteData(final int index, final int size) {
            // Set the offset and validity buffer
            final int offset = m_vector.startNewValue(m_offset + index);
            m_vector.endValue(m_offset + index, size);

            // Make sure the value data is big enough
            final int lastIndex = offset + size;
            if (m_data.capacity() < lastIndex) {
                m_data.expand(lastIndex);
            }

            // Set the slice in the value data
            @SuppressWarnings("unchecked")
            final C data = (C)m_data.slice(offset);
            return data;
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfList(m_vector) + m_data.sizeOf();
        }

        @Override
        public ArrowListWriteData slice(final int start) {
            return new ArrowListWriteData(m_vector, m_data, m_offset + start);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowListReadData close(final int length) {
            final ArrowBuf offsetBuffer = m_vector.getOffsetBuffer();
            final long nextToSet = m_vector.getLastSet() + 1L;

            // Fill to the end
            final int dataLength = offsetBuffer.getInt(nextToSet * BaseRepeatedValueVector.OFFSET_WIDTH);
            for (long i = nextToSet + 1; i < length - 1; i++) {
                offsetBuffer.setInt(i * BaseRepeatedValueVector.OFFSET_WIDTH, dataLength);
            }

            final OffHeapArrowReadData readData = m_data.close(dataLength);
            final ListVector vector = closeWithLength(length);
            return new ArrowListReadData(vector, MissingValues.forValidityBuffer(vector.getValidityBuffer(), length),
                readData);
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_data.release();
        }
    }

    /** Arrow implementation of {@link ListReadData}. */
    public static final class ArrowListReadData extends AbstractOffHeapArrowReadData<ListVector> implements ListReadData {

        // Package private for testing only
        final OffHeapArrowReadData m_data;

        private ArrowListReadData(final ListVector vector, final MissingValues missingValues,
            final OffHeapArrowReadData data) {
            super(vector, missingValues);
            m_data = data;
        }

        private ArrowListReadData(final ListVector vector, final MissingValues missingValues, final OffHeapArrowReadData data,
            final int offset, final int length) {
            super(vector, missingValues, offset, length);
            m_data = data;
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfList(m_vector) + m_data.sizeOf();
        }

        @Override
        public ArrowListReadData slice(final int start, final int length) {
            return new ArrowListReadData(m_vector, m_missingValues, m_data, m_offset + start, length);
        }

        @Override
        public <C extends NullableReadData> C createReadData(final int index) {
            // Slice the data
            final int start = m_vector.getElementStartIndex(m_offset + index);
            final int length = m_vector.getElementEndIndex(m_offset + index) - start;
            @SuppressWarnings("unchecked")
            final C data = (C)m_data.slice(start, length);
            return data;
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_data.release();
        }
    }

    /**
     * Implementation of {@link OffHeapArrowColumnDataFactory} for {@link ArrowListReadData} and {@link ArrowListWriteData}.
     */
    public static final class ArrowListDataFactory extends AbstractOffHeapArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private static final int INITIAL_VALUES_PER_LIST = 10;

        private final OffHeapArrowColumnDataFactory m_inner;

        /**
         * Create a new factory for Arrow list data.
         *
         * @param inner the factory to create the type of the list elements
         */
        public ArrowListDataFactory(final OffHeapArrowColumnDataFactory inner) {
            super(ArrowColumnDataFactoryVersion.version(CURRENT_VERSION, inner.getVersion()));
            m_inner = inner;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final Field data = m_inner.getField("listData", dictionaryIdSupplier);
            return new Field(name, new FieldType(true, MinorType.LIST.getType(), null),
                Collections.singletonList(data));
        }

        @Override
        @SuppressWarnings("resource") // Data vector closed with list vector
        public ArrowListWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final ListVector v = (ListVector)vector;
            // Note: we must do that before creating the inner data because "allocateNew" overwrites the allocation for
            // the child vector
            v.setInitialCapacity(capacity);
            v.allocateNew();

            // Data vector
            final FieldVector dataVector = v.getDataVector();
            final OffHeapArrowWriteData data =
                m_inner.createWrite(dataVector, dictionaryIdSupplier, allocator, capacity * INITIAL_VALUES_PER_LIST);
            return new ArrowListWriteData(v, data);
        }

        @Override
        @SuppressWarnings("resource") // Data vector closed with list vector
        public ArrowListReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (version.getVersion() == CURRENT_VERSION) {
                final ListVector v = (ListVector)vector;

                // Data vector
                final FieldVector dataVector = v.getDataVector();
                final OffHeapArrowReadData data =
                    m_inner.createRead(dataVector, nullCount.getChild(0), provider, version.getChildVersion(0));

                return new ArrowListReadData(v, MissingValues.forNullCount(nullCount.getNullCount(), v.getValueCount()),
                    data);
            } else {
                throw new IOException("Cannot read ArrowListData with version " + version.getVersion()
                    + ". Current version: " + CURRENT_VERSION + ".");
            }
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            final ArrowListReadData d = (ArrowListReadData)data;
            return m_inner.getDictionaries(d.m_data);
        }

        @Override
        public boolean equals(final Object obj) {

            if (!super.equals(obj)) {
                return false;
            }
            final ArrowListDataFactory o = (ArrowListDataFactory)obj;
            return Objects.equals(m_inner, o.m_inner);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_version, m_inner);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + ".v" + CURRENT_VERSION + "[" + m_inner + "]";
        }

        @Override
        public int initialNumBytesPerElement() {
            return m_inner.initialNumBytesPerElement() * INITIAL_VALUES_PER_LIST // data buffer
                + BaseRepeatedValueVector.OFFSET_WIDTH // offset buffer
                + 1; // validity bit
        }
    }
}
