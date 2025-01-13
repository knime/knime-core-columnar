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
 *   Sep 24, 2020 (dietzc): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractReferencedData;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.VoidData.VoidReadData;
import org.knime.core.columnar.data.VoidData.VoidWriteData;

/**
 * Arrow implementation of {@link VoidWriteData} and {@link VoidReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowVoidData extends AbstractReferencedData
    implements VoidReadData, VoidWriteData, OnHeapArrowReadData, OnHeapArrowWriteData {

    private int m_capacity;

    private OnHeapArrowVoidData(final int capacity) {
        m_capacity = capacity;
    }

    @Override
    public int capacity() {
        return m_capacity;
    }

    @Override
    public long sizeOf() {
        return 0;
    }

    @Override
    public long usedSizeFor(final int numElements) {
        return 0;
    }

    @Override
    public void expand(final int minimumCapacity) {
        m_capacity = minimumCapacity;
    }

    @Override
    public OnHeapArrowVoidData close(final int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length must be non-negative.");
        }
        if (length > capacity()) {
            throw new IllegalArgumentException("Length must not be larger than capacity.");
        }
        m_capacity = length;
        return this;
    }

    @Override
    public int length() {
        return m_capacity;
    }

    @Override
    public OnHeapArrowWriteData slice(final int start) {
        return new OnHeapArrowVoidData(m_capacity);
    }

    @Override
    public OnHeapArrowVoidData slice(final int start, final int length) {
        return new OnHeapArrowVoidData(length);
    }

    @Override
    public ValidityBuffer getValidityBuffer() {
        return null;
    }

    @Override
    protected void closeResources() {
        m_capacity = 0;
    }

    /** Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowVoidData} */
    public static final class ArrowVoidDataFactory extends AbstractOnHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowVoidDataFactory} */
        public static final ArrowVoidDataFactory INSTANCE = new ArrowVoidDataFactory();

        private ArrowVoidDataFactory() {
            super(0);
        }

        @Override
        public OnHeapArrowVoidData createWrite(final int capacity) {
            return new OnHeapArrowVoidData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return new Field(name, new FieldType(false, MinorType.NULL.getType(), null), null);
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector fieldVector) {
            fieldVector.setValueCount(data.length());
        }

        @Override
        public OnHeapArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new OnHeapArrowVoidData(vector.getValueCount());
            } else {
                throw new IOException(
                    "Cannot read ArrowVoidData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return 0;
        }
    }
}
