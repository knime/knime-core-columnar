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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.NestedDictionaryProvider;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;

/**
 * Arrow implementation of {@link StructWriteData} and {@link StructReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowStructData {

    private ArrowStructData() {
    }

    /** Arrow implementation of {@link StructWriteData}. */
    public static final class ArrowStructWriteData extends AbstractArrowWriteData<StructVector>
        implements StructWriteData {

        private final ArrowWriteData[] m_children;

        private ArrowStructWriteData(final StructVector vector, final ArrowWriteData... children) {
            super(vector);
            m_children = children;
        }

        private ArrowStructWriteData(final StructVector vector, final int offset, final ArrowWriteData... children) {
            super(vector, offset);
            m_children = children;
        }

        @Override
        public void setMissing(final int index) {
            // NB: We don't need to call m_vector.setNull because it is always null until #close
            // Set all children to missing
            for (final ColumnWriteData child : m_children) {
                child.setMissing(m_offset + index);
            }
        }

        @Override
        public long sizeOf() {
            return ArrowStructData.sizeOf(m_vector, m_children);
        }

        @Override
        public <C extends ColumnWriteData> C getWriteDataAt(final int index) {
            @SuppressWarnings("unchecked")
            final C cast = (C)m_children[index];
            return cast;
        }

        @Override
        public ArrowStructWriteData slice(final int start) {
            final ArrowWriteData[] slicedChildren = new ArrowWriteData[m_children.length];
            for (int i = 0; i < m_children.length; i++) {
                slicedChildren[i] = m_children[i].slice(start);
            }
            return new ArrowStructWriteData(m_vector, m_offset + start, slicedChildren);
        }

        @Override
        @SuppressWarnings("resource") // Validity buffer and child vectors closed by m_vector, vector closed by ReadData
        public ArrowStructReadData close(final int length) {
            final int numChildren = m_children.length;

            // Close the children
            final ArrowReadData[] readChildren = new ArrowReadData[numChildren];
            for (int i = 0; i < numChildren; i++) {
                readChildren[i] = m_children[i].close(length);
            }

            // Set the validity buffer
            final ArrowBuf validityBuffer = m_vector.getValidityBuffer();

            // Get the validity buffers of the children
            final List<FieldVector> childVectors = m_vector.getChildrenFromFields();
            final ArrowBuf[] childValidtiyBuffers = new ArrowBuf[childVectors.size()];
            for (int i = 0; i < childVectors.size(); i++) {
                final FieldVector child = childVectors.get(i);
                childValidtiyBuffers[i] = child.getValidityBuffer();
            }

            // Loop over the buffers and set the validity buffer to the result of a bitwise or operation
            // NB: We assume our buffer is padded to a multiple of 4
            final int bytesToSet = (int)Math.ceil(length / 8.0);
            for (int bufferPosition = 0; bufferPosition < bytesToSet; bufferPosition += 4) {
                int validity = 0;
                // NB: if validity is -1 all bits are set and we do not need to check the other buffers
                for (int i = 0; i < childValidtiyBuffers.length && validity != -1; i++) {
                    validity = validity | childValidtiyBuffers[i].getInt(bufferPosition);
                }
                validityBuffer.setInt(bufferPosition, validity);
            }

            return new ArrowStructReadData(closeWithLength(length), readChildren);
        }

        @Override
        protected void closeResources() {
            ArrowStructData.closeResources(m_vector, m_children);
        }
    }

    /** Arrow implementation of {@link StructReadData}. */
    public static final class ArrowStructReadData extends AbstractArrowReadData<StructVector>
        implements StructReadData {

        private final ArrowReadData[] m_children;

        private ArrowStructReadData(final StructVector vector, final ArrowReadData... children) {
            super(vector);
            m_children = children;
        }

        private ArrowStructReadData(final StructVector vector, final int offset, final int length,
            final ArrowReadData... children) {
            super(vector, offset, length);
            m_children = children;
        }

        @Override
        public long sizeOf() {
            return ArrowStructData.sizeOf(m_vector, m_children);
        }

        @Override
        public <C extends ColumnReadData> C getReadDataAt(final int index) {
            @SuppressWarnings("unchecked")
            final C cast = (C)m_children[index];
            return cast;
        }

        @Override
        public ArrowStructReadData slice(final int start, final int length) {
            final ArrowReadData[] slicedChildren = new ArrowReadData[m_children.length];
            for (int i = 0; i < m_children.length; i++) {
                slicedChildren[i] = m_children[i].slice(start, length);
            }
            return new ArrowStructReadData(m_vector, m_offset + start, length, slicedChildren);
        }

        @Override
        protected void closeResources() {
            ArrowStructData.closeResources(m_vector, m_children);
        }
    }

    private static long sizeOf(final StructVector vector, final ReferencedData[] children) {
        long size = ArrowSizeUtils.sizeOfStruct(vector);
        for (final ReferencedData c : children) {
            size += c.sizeOf();
        }
        return size;
    }

    private static void closeResources(final StructVector vector, final ReferencedData[] children) {
        if (vector != null) {
            vector.close();
            for (final ReferencedData c : children) {
                c.release();
            }
        }
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link ArrowStructReadData} and
     * {@link ArrowStructWriteData}.
     */
    public static final class ArrowStructDataFactory extends AbstractArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private final ArrowColumnDataFactory[] m_inner;

        private static ArrowColumnDataFactoryVersion[] getVersions(final ArrowColumnDataFactory[] factories) {
            final ArrowColumnDataFactoryVersion[] versions = new ArrowColumnDataFactoryVersion[factories.length];
            for (int i = 0; i < factories.length; i++) {
                versions[i] = factories[i].getVersion();
            }
            return versions;
        }

        /**
         * Create a new factory for Arrow struct data.
         *
         * @param inner factories to create the inner types
         */
        public ArrowStructDataFactory(final ArrowColumnDataFactory... inner) {
            super(ArrowColumnDataFactoryVersion.version(CURRENT_VERSION, getVersions(inner)));
            m_inner = inner;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final List<Field> children = new ArrayList<>(m_inner.length);
            for (int i = 0; i < m_inner.length; i++) {
                children.add(m_inner[i].getField(childNameAtIndex(i), dictionaryIdSupplier));
            }
            return new Field(name, new FieldType(true, Struct.INSTANCE, null), children);
        }

        @Override
        public ArrowStructWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final StructVector v = (StructVector)vector;
            // Children
            final ArrowWriteData[] children = new ArrowWriteData[m_inner.length];
            for (int i = 0; i < children.length; i++) {
                @SuppressWarnings("resource") // Child vector closed with struct vector
                final FieldVector childVector = v.getChild(childNameAtIndex(i));
                children[i] = m_inner[i].createWrite(childVector, dictionaryIdSupplier, allocator, capacity);
            }
            v.setInitialCapacity(capacity);
            v.allocateNew();
            return new ArrowStructWriteData(v, children);
        }

        @Override
        public ArrowStructReadData createRead(final FieldVector vector, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            if (version.getVersion() == CURRENT_VERSION) {
                final StructVector v = (StructVector)vector;

                // Children
                final ArrowReadData[] children = new ArrowReadData[m_inner.length];
                for (int i = 0; i < children.length; i++) {
                    @SuppressWarnings("resource") // Child vector closed with struct vector
                    final FieldVector childVector = v.getChild(childNameAtIndex(i));
                    children[i] = m_inner[i].createRead(childVector, provider, version.getChildVersion(i));
                }

                return new ArrowStructReadData(v, children);
            } else {
                throw new IOException("Cannot read ArrowStructData with version " + version.getVersion()
                    + ". Current version: " + CURRENT_VERSION + ".");
            }
        }

        @Override
        public DictionaryProvider getDictionaries(final ColumnReadData data) {
            final ArrowStructReadData d = (ArrowStructReadData)data;
            final List<DictionaryProvider> providers = new ArrayList<>();
            for (int i = 0; i < m_inner.length; i++) {
                final DictionaryProvider p = m_inner[i].getDictionaries(d.getReadDataAt(i));
                if (p != null) {
                    providers.add(p);
                }
            }
            return new NestedDictionaryProvider(providers);
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof ArrowStructDataFactory)) {
                return false;
            }
            final ArrowStructDataFactory o = (ArrowStructDataFactory)obj;
            return m_version.equals(o.m_version) && Arrays.equals(m_inner, o.m_inner);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_version, m_inner);
        }

        private static final String childNameAtIndex(final int index) {
            return String.valueOf(index);
        }
    }
}
