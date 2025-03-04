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
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;

/**
 * Arrow implementation of {@link StructWriteData} and {@link StructReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowStructData {

    private OnHeapArrowStructData() {
    }

    /** Arrow implementation of {@link StructWriteData}. */
    public static final class ArrowStructWriteData
        extends AbstractOnHeapArrowWriteData<OnHeapArrowWriteData[], ArrowStructReadData> implements StructWriteData {

        private ArrowStructWriteData(final int capacity, final OnHeapArrowWriteData[] data) {
            // Note that we set the validity buffer to "null" because the validity is given by the children
            super(data, null, 0, capacity);
            m_capacity = capacity;
        }

        private ArrowStructWriteData(final OnHeapArrowWriteData[] data, final int offset, final int capacity) {
            super(data, null, offset, capacity);
            m_capacity = capacity;
        }

        @Override
        public <C extends NullableWriteData> C getWriteDataAt(final int index) {
            @SuppressWarnings("unchecked")
            C result = (C)m_data[index];
            return result;
        }

        @Override
        public ArrowStructWriteData slice(final int start) {
            // Create sliced children
            OnHeapArrowWriteData[] slicedChildren = new OnHeapArrowWriteData[m_data.length];
            for (int i = 0; i < m_data.length; i++) {
                slicedChildren[i] = m_data[i].slice(start);
            }
            return new ArrowStructWriteData(slicedChildren, m_offset + start, m_capacity - start);
        }

        @Override
        public void setMissing(final int index) {
            // Also set all children to missing
            for (NullableWriteData child : m_data) {
                child.setMissing(index);
            }
        }

        @Override
        protected void setValid(final int index) {
            // Overwritten to ensure that we do not get a NullPointerException by calling set on the validity buffer
            // which is null
            // The validity is given by the children and therefore this implementation is empty
        }

        @Override
        public long usedSizeFor(final int numElements) {
            long size = 0;
            for (var child : m_data) {
                size += child.usedSizeFor(numElements);
            }
            return size;
        }

        @Override
        public long sizeOf() {
            long size = 0;
            for (var child : m_data) {
                size += child.sizeOf();
            }
            return size;
        }

        @Override
        protected void closeResources() {
            if (m_data != null) {
                for (var child : m_data) {
                    child.release();
                }
            }
            super.closeResources();
        }

        @Override
        public ArrowStructReadData close(final int length) {
            // Note that we do not need to set the length of the children, as they are set in the close method
            var readData = createReadData(length);
            closeResources();
            return readData;
        }

        @Override
        protected ArrowStructReadData createReadData(final int length) {
            // Close children and create read children
            var readChildren = new OnHeapArrowReadData[m_data.length];
            var validityBuffers = new ValidityBuffer[m_data.length];
            for (int i = 0; i < m_data.length; i++) {
                readChildren[i] = m_data[i].close(length);
                validityBuffers[i] = readChildren[i].getValidityBuffer();
            }

            var validity = new ValidityBuffer(length);
            validity.setFrom(validityBuffers);

            return new ArrowStructReadData(readChildren, validity, length);
        }

        @Override
        protected void setNumElements(final int numElements) {
            for (NullableWriteData child : m_data) {
                child.expand(numElements);
            }
            m_capacity = numElements;
        }
    }

    /** Arrow implementation of {@link StructReadData}. */
    public static final class ArrowStructReadData extends AbstractOnHeapArrowReadData<OnHeapArrowReadData[]>
        implements StructReadData {

        private ArrowStructReadData(final OnHeapArrowReadData[] data, final ValidityBuffer validity, final int length) {
            super(data, validity, length);
        }

        private ArrowStructReadData(final OnHeapArrowReadData[] data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(data, validity, offset, length);
        }

        @Override
        public <C extends NullableReadData> C getReadDataAt(final int index) {
            @SuppressWarnings("unchecked")
            C result = (C)m_data[index];
            return result;
        }

        @Override
        public OnHeapArrowReadData slice(final int start, final int length) {
            var slicedChildren = new OnHeapArrowReadData[m_data.length];
            for (int i = 0; i < m_data.length; i++) {
                slicedChildren[i] = m_data[i].slice(start, length);
            }
            return new ArrowStructReadData(slicedChildren, m_validity, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            long size = m_validity.sizeOf();
            for (var child : m_data) {
                size += child.sizeOf();
            }
            return size;
        }

        @Override
        protected void closeResources() {
            if (m_data != null) {
                for (var child : m_data) {
                    child.release();
                }
            }
            super.closeResources();
        }
    }

    /**
     * Implementation of {@link OnHeapArrowColumnDataFactory} for {@link ArrowStructReadData} and
     * {@link ArrowStructWriteData}.
     */
    public static final class ArrowStructDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 1;

        /**
         * Also covers support for legacy date&time data types.
         */
        private static final int V0 = 0;

        /**
         * Create a new factory for Arrow struct data.
         *
         * @param inner factories to create the inner types
         */
        public ArrowStructDataFactory(final OnHeapArrowColumnDataFactory... inner) {
            super(CURRENT_VERSION, inner);
        }

        @Override
        public ArrowStructWriteData createWrite(final int capacity) {
            var children = new OnHeapArrowWriteData[m_children.length];
            for (int i = 0; i < m_children.length; i++) {
                children[i] = m_children[i].createWrite(capacity);
            }
            return new ArrowStructWriteData(capacity, children);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            List<Field> children = new ArrayList<>(m_children.length);
            for (int i = 0; i < m_children.length; i++) {
                children.add(m_children[i].getField(childNameAtIndex(i), dictionaryIdSupplier));
            }
            return new Field(name, new FieldType(true, MinorType.STRUCT.getType(), null), children);
        }

        @Override
        @SuppressWarnings("resource") // buffers and child vectors are owned by the vector
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            ArrowStructReadData d = (ArrowStructReadData)data;
            d.checkNotSliced();
            StructVector sv = (StructVector)vector;

            sv.setInitialCapacity(d.length());
            sv.allocateNew();

            d.m_validity.copyTo(sv.getValidityBuffer());

            for (int i = 0; i < m_children.length; i++) {
                var childVector = (FieldVector)sv.getChildByOrdinal(i);
                m_children[i].copyToVector(d.getReadDataAt(i), childVector);
            }

            sv.setValueCount(d.length());
        }

        @Override
        @SuppressWarnings("resource") // buffers and child vectors are owned by the vector
        public ArrowStructReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            var childVersions = version.getChildVersions();
            if (version.getVersion() == V0) {
                if (childVersions.length == 0) {
                    // in case of legacy date&time data we used Arrow structs directly and hence there are no versions
                    // for the children, however, we know that all versions were 0 (and that there was no more nesting)
                    childVersions = Stream.generate(() -> ArrowColumnDataFactoryVersion.version(0)) //
                        .limit(m_children.length) //
                        .toArray(ArrowColumnDataFactoryVersion[]::new);
                }
            } else if (version.getVersion() != CURRENT_VERSION) {
                throw new IOException("Cannot read ArrowStructData with version " + version.getVersion()
                    + ". Current version: " + CURRENT_VERSION + ".");
            }

            // NOTE: This code is valid for V0 and V1 (CURRENT_VERSION)
            // (they only differ for the saved child versions which is handled above)
            StructVector v = (StructVector)vector;
            int valueCount = v.getValueCount();
            ValidityBuffer validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

            var children = new OnHeapArrowReadData[m_children.length];
            for (int i = 0; i < m_children.length; i++) {
                var childVector = (FieldVector)v.getChildByOrdinal(i);
                children[i] = m_children[i].createRead(childVector, nullCount.getChild(i), provider, childVersions[i]);
            }

            return new ArrowStructReadData(children, validity, valueCount);
        }

        @Override
        public int initialNumBytesPerElement() {
            int sum = 0;
            for (var f : m_children) {
                sum += f.initialNumBytesPerElement();
            }
            return sum;
        }

        private static String childNameAtIndex(final int index) {
            return String.valueOf(index);
        }
    }
}
