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
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;

/**
 * Arrow implementation of {@link ListWriteData} and {@link ListReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowListData {

    private ArrowListData() {
    }

    /** Arrow implementation of {@link ListWriteData}. */
    public static final class ArrowListWriteData extends AbstractReferenceData
        implements ListWriteData, ArrowWriteData {

        // Package private for testing only
        final ListVector m_vector;

        // Package private for testing only
        final ArrowWriteData m_data;

        private int m_offset;

        private ArrowListWriteData(final ListVector vector, final ArrowWriteData data) {
            m_vector = vector;
            m_data = data;
        }

        @Override
        public void setMissing(final int index) {
            @SuppressWarnings("resource") // Validity buffer handled by vector
            final ArrowBuf validityBuffer = m_vector.getValidityBuffer();
            BitVectorHelper.unsetBit(validityBuffer, m_offset + index);
        }

        @Override
        public int capacity() {
            return m_vector.getValueCapacity();
        }

        @Override
        public void expand(final int minimumCapacity) {
            while (m_vector.getValueCapacity() < minimumCapacity) {
                m_vector.reAlloc();
            }
        }

        @Override
        public <C extends ColumnWriteData> C getWriteData(final int index, final int size) {
            // Set the offset and validity buffer
            final int offset = m_vector.startNewValue(m_offset + index);
            m_vector.endValue(m_offset + index, size);

            // Make sure the value data is big enough
            final int lastIndex = offset + size;
            if (m_data.capacity() < lastIndex) {
                m_data.expand(lastIndex);
            }

            // Set the slice in the value data
            m_data.slice(offset, size);

            @SuppressWarnings("unchecked")
            final C data = (C)m_data;
            return data;
        }

        @Override
        public int sizeOf() {
            return ArrowListData.sizeOf(m_vector, m_data);
        }

        @Override
        public void slice(final int start, final int length) {
            m_offset = start;
        }

        @Override
        public ArrowListReadData close(final int length) {
            final int lastEndIndex = m_vector.getElementEndIndex(length - 1);
            final ArrowReadData readData = m_data.close(lastEndIndex + 1);
            m_vector.setValueCount(length);
            return new ArrowListReadData(m_vector, readData);
        }

        @Override
        protected void closeResources() {
            ArrowListData.closeResources(m_vector, m_data);
        }

        @Override
        public String toString() {
            return ArrowListData.toString(m_vector, m_offset, m_vector.getValueCount());
        }
    }

    /** Arrow implementation of {@link ListReadData}. */
    public static final class ArrowListReadData extends AbstractReferenceData implements ListReadData, ArrowReadData {

        // Package private for testing only
        final ListVector m_vector;

        // Package private for testing only
        final ArrowReadData m_data;

        private int m_offset;

        private int m_length;

        private ArrowListReadData(final ListVector vector, final ArrowReadData data) {
            m_vector = vector;
            m_data = data;
            m_offset = 0;
            m_length = vector.getValueCount();
        }

        @Override
        public boolean isMissing(final int index) {
            return m_vector.isNull(m_offset + index);
        }

        @Override
        public int length() {
            return m_length;
        }

        @Override
        public int sizeOf() {
            return ArrowListData.sizeOf(m_vector, m_data);
        }

        @Override
        public void slice(final int start, final int length) {
            m_offset = start;
            m_length = length;
        }

        @Override
        public <C extends ColumnReadData> C getReadData(final int index) {
            // Slice the data
            final int start = m_vector.getElementStartIndex(m_offset + index);
            final int length = m_vector.getElementEndIndex(m_offset + index) - start;
            m_data.slice(start, length);

            @SuppressWarnings("unchecked")
            final C data = (C)m_data;
            return data;
        }

        @Override
        protected void closeResources() {
            ArrowListData.closeResources(m_vector, m_data);
        }

        @Override
        public String toString() {
            return ArrowListData.toString(m_vector, m_offset, m_offset + m_length);
        }
    }

    @SuppressWarnings("resource") // Offset and validity buffers are handled by the vector
    private static int sizeOf(final ListVector vector, final ReferencedData data) {
        return (int)(vector.getOffsetBuffer().capacity() + vector.getValidityBuffer().capacity() + data.sizeOf());
    }

    private static void closeResources(final ListVector vector, final ReferencedData data) {
        vector.close();
        data.release();
    }

    private static String toString(final ListVector vector, final int start, final int end) {
        return ValueVectorUtility.getToString(vector, start, end);
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link ArrowListReadData} and {@link ArrowListWriteData}.
     */
    public static final class ArrowListDataFactory implements ArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private static final int INITIAL_VALUES_PER_LIST = 10;

        private final ArrowColumnDataFactory m_inner;

        private final ArrowColumnDataFactoryVersion m_version;

        /**
         * Create a new factory for Arrow list data.
         *
         * @param inner the factory to create the type of the list elements
         */
        public ArrowListDataFactory(final ArrowColumnDataFactory inner) {
            m_inner = inner;
            m_version = ArrowColumnDataFactoryVersion.version(CURRENT_VERSION, inner.getVersion());
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
            // Data vector
            final FieldVector dataVector = v.getDataVector();
            final ArrowWriteData data =
                m_inner.createWrite(dataVector, dictionaryIdSupplier, allocator, capacity * INITIAL_VALUES_PER_LIST);
            v.setInitialCapacity(capacity);
            v.allocateNew();
            return new ArrowListWriteData(v, data);
        }

        @Override
        @SuppressWarnings("resource") // Data vector closed with list vector
        public ArrowListReadData createRead(final FieldVector vector, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            if (version.getVersion() == CURRENT_VERSION) {
                final ListVector v = (ListVector)vector;

                // Data vector
                final FieldVector dataVector = v.getDataVector();
                final ArrowReadData data = m_inner.createRead(dataVector, provider, version.getChildVersion(0));

                return new ArrowListReadData(v, data);
            } else {
                throw new IOException("Cannot read ArrowListData with version " + version.getVersion()
                    + ". Current version: " + CURRENT_VERSION + ".");
            }
        }

        @Override
        public ListVector getVector(final ColumnReadData data) {
            return ((ArrowListReadData)data).m_vector;
        }

        @Override
        public DictionaryProvider getDictionaries(final ColumnReadData data) {
            final ArrowListReadData d = (ArrowListReadData)data;
            return m_inner.getDictionaries(d.m_data);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return m_version;
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof ArrowListDataFactory)) {
                return false;
            }
            final ArrowListDataFactory o = (ArrowListDataFactory)obj;
            return Objects.equals(m_version, o.m_version) && Objects.equals(m_inner, o.m_inner);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_version, m_inner);
        }
    }
}
