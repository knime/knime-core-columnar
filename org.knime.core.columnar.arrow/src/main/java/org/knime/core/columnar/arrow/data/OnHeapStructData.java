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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.NestedDictionaryProvider;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;

/**
 * An on-heap implementation of Struct data. Similar to OnHeapListData and OnHeapVarBinaryData, this class stores its
 * data entirely in Java arrays and buffers, rather than relying directly on Arrow vectors.
 */
public final class OnHeapStructData {

    public static OnHeapStructDataFactory factory(final ArrowColumnDataFactory... inner) {
        return new OnHeapStructDataFactory(inner);
    }

    private OnHeapStructData() {
    }

    /** On-heap implementation of {@link StructWriteData}. */
    public static final class OnHeapStructWriteData extends AbstractArrowWriteData implements StructWriteData {

        private final ArrowWriteData[] m_children;

        private int m_capacity;

        private OnHeapStructWriteData(final int capacity, final ArrowWriteData[] children) {
            super(capacity);
            m_capacity = capacity;
            m_children = children;
        }

        private OnHeapStructWriteData(final int offset, final ValidityBuffer validity, final ArrowWriteData[] children,
            final int capacity) {
            super(offset, validity);
            m_capacity = capacity;
            m_children = children;
        }

        @Override
        public int capacity() {
            return m_capacity;
        }

        @Override
        public void expand(final int minimumCapacity) {
            if (minimumCapacity > m_capacity) {
                m_validity.setNumElements(minimumCapacity);
                for (NullableWriteData child : m_children) {
                    child.expand(minimumCapacity);
                }
                m_capacity = minimumCapacity;
            }
        }

        @Override
        public void setMissing(final int index) {
            m_validity.set(m_offset + index, false);
            // Also set all children to missing
            for (NullableWriteData child : m_children) {
                child.setMissing(index);
            }
        }

        @Override
        public long usedSizeFor(final int numElements) {
            long size = ValidityBuffer.usedSizeFor(numElements);
            for (var child : m_children) {
                size += child.usedSizeFor(numElements);
            }
            return size;
        }

        @Override
        public long sizeOf() {
            return usedSizeFor(m_capacity);
        }

        @Override
        public OnHeapStructReadData close(final int length) {
            // Close children and create read children
            var readChildren = new ArrowReadData[m_children.length];
            var validityBuffers = new ValidityBuffer[m_children.length];
            for (int i = 0; i < m_children.length; i++) {
                readChildren[i] = m_children[i].close(length);
                validityBuffers[i] = readChildren[i].getValidityBuffer();
            }

            m_validity.setNumElements(length);
            m_validity.setFrom(validityBuffers);

            return new OnHeapStructReadData(readChildren, m_validity, length);
        }

        @Override
        public OnHeapStructWriteData slice(final int start) {
            // Create sliced children
            ArrowWriteData[] slicedChildren = new ArrowWriteData[m_children.length];
            for (int i = 0; i < m_children.length; i++) {
                slicedChildren[i] = m_children[i].slice(start);
            }
            return new OnHeapStructWriteData(m_offset + start, m_validity, slicedChildren, m_capacity - start);
        }

        @Override
        public <C extends NullableWriteData> C getWriteDataAt(final int index) {
            @SuppressWarnings("unchecked")
            C result = (C)m_children[index];
            return result;
        }

        @Override
        protected void closeResources() {
            // No extra resources
        }
    }

    /** On-heap implementation of {@link StructReadData}. */
    public static final class OnHeapStructReadData extends AbstractArrowReadData implements StructReadData {

        private final ArrowReadData[] m_children;

        private OnHeapStructReadData(final ArrowReadData[] children, final ValidityBuffer validity, final int length) {
            super(validity, length);
            m_children = children;
        }

        private OnHeapStructReadData(final ArrowReadData[] children, final ValidityBuffer validity, final int offset,
            final int length) {
            super(validity, offset, length);
            m_children = children;
        }

        @Override
        public long sizeOf() {
            long size = m_validity.sizeOf();
            for (var child : m_children) {
                size += child.sizeOf();
            }
            return size;
        }

        @Override
        public <C extends NullableReadData> C getReadDataAt(final int index) {
            @SuppressWarnings("unchecked")
            C result = (C)m_children[index];
            return result;
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            var slicedChildren = new ArrowReadData[m_children.length];
            for (int i = 0; i < m_children.length; i++) {
                slicedChildren[i] = m_children[i].slice(start, length);
            }
            return new OnHeapStructReadData(slicedChildren, m_validity, m_offset + start, length);
        }

        @Override
        protected void closeResources() {
            // No extra resources
        }
    }

    public static final class OnHeapStructDataFactory extends AbstractArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private final ArrowColumnDataFactory[] m_inner;

        private OnHeapStructDataFactory(final ArrowColumnDataFactory... inner) {
            super(ArrowColumnDataFactoryVersion.version(CURRENT_VERSION, childVersions(inner)));
            m_inner = inner;
        }

        // <<<<< START
        // TODO move child handling to abstract parent - it can be empty for factorys withouth children

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), m_inner);
        }

        @Override
        public boolean equals(final Object obj) {
            return super.equals(obj) && obj instanceof OnHeapStructDataFactory
                && ((OnHeapStructDataFactory)obj).m_inner.equals(m_inner);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + ".v" + m_version.getVersion() + Arrays.toString(m_inner);
        }

        // <<<<< END

        private static ArrowColumnDataFactoryVersion[] childVersions(final ArrowColumnDataFactory[] factories) {
            ArrowColumnDataFactoryVersion[] versions = new ArrowColumnDataFactoryVersion[factories.length];
            for (int i = 0; i < factories.length; i++) {
                versions[i] = factories[i].getVersion();
            }
            return versions;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            List<Field> children = new ArrayList<>(m_inner.length);
            for (int i = 0; i < m_inner.length; i++) {
                children.add(m_inner[i].getField(childNameAtIndex(i), dictionaryIdSupplier));
            }
            return new Field(name, new FieldType(true, MinorType.STRUCT.getType(), null), children);
        }

        @Override
        public OnHeapStructWriteData createWrite(final int capacity) {
            var children = new ArrowWriteData[m_inner.length];
            for (int i = 0; i < m_inner.length; i++) {
                children[i] = m_inner[i].createWrite(capacity);
            }
            return new OnHeapStructWriteData(capacity, children);
        }

        @Override
        public OnHeapStructReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (!(vector instanceof StructVector)) {
                throw new IOException("Expected a StructVector, but got: " + vector.getClass().getSimpleName());
            }
            StructVector sv = (StructVector)vector;
            int valueCount = sv.getValueCount();
            ValidityBuffer validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

            var children = new ArrowReadData[m_inner.length];
            for (int i = 0; i < m_inner.length; i++) {
                var childVector = (FieldVector)sv.getChildByOrdinal(i);
                children[i] =
                    m_inner[i].createRead(childVector, nullCount.getChild(i), provider, version.getChildVersion(i));
            }

            return new OnHeapStructReadData(children, validity, valueCount);
        }

        @Override
        public int initialNumBytesPerElement() {
            int sum = 0;
            for (ArrowColumnDataFactory f : m_inner) {
                sum += f.initialNumBytesPerElement();
            }
            return sum;
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            OnHeapStructReadData d = (OnHeapStructReadData)data;
            StructVector sv = (StructVector)vector;

            sv.setInitialCapacity(d.length());
            sv.allocateNew();

            d.m_validity.copyTo(sv.getValidityBuffer());

            for (int i = 0; i < m_inner.length; i++) {
                FieldVector childVector = (FieldVector)sv.getChildByOrdinal(i);
                m_inner[i].copyToVector(d.getReadDataAt(i), childVector);
            }

            sv.setValueCount(d.length());
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            OnHeapStructReadData d = (OnHeapStructReadData)data;
            List<DictionaryProvider> providers = new ArrayList<>();
            for (int i = 0; i < m_inner.length; i++) {
                DictionaryProvider p = m_inner[i].getDictionaries(d.getReadDataAt(i));
                if (p != null) {
                    providers.add(p);
                }
            }
            if (providers.isEmpty()) {
                return null;
            } else if (providers.size() == 1) {
                return providers.get(0);
            } else {
                return new NestedDictionaryProvider(providers);
            }
        }

        private static String childNameAtIndex(final int index) {
            return String.valueOf(index);
        }
    }
}
