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
package org.knime.core.columnar.arrow.offheap.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.offheap.OffHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.data.AbstractOffHeapArrowReadData.MissingValues;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.table.schema.DataSpec;

/**
 * Arrow implementation of {@link IntWriteData} and {@link IntReadData} representing unsigned values.
 *
 * Only to represent dictionary keys where we want to use the full range of 32 bits for non-negative numbers. There is
 * no {@link DataSpec} available for unsigned integers, because Java does not support these as primitive types.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
final class OffHeapArrowUnsignedIntData {

    private OffHeapArrowUnsignedIntData() {
    }

    /** Arrow implementation of unsigned {@link IntWriteData}. */
    public static final class ArrowUnsignedIntWriteData extends AbstractOffHeapArrowWriteData<UInt4Vector>
        implements IntWriteData {

        private ArrowUnsignedIntWriteData(final UInt4Vector vector) {
            super(vector);
        }

        private ArrowUnsignedIntWriteData(final UInt4Vector vector, final int offset) {
            super(vector, offset);
        }

        /**
         * The signed integer value will be interpreted as unsigned int, meaning all negative values will be mapped to
         * values larger than {@link Integer#MAX_VALUE}.
         */
        @Override
        public void setInt(final int index, final int val) {
            m_vector.set(m_offset + index, val);
        }

        @Override
        public OffHeapArrowWriteData slice(final int start) {
            return new ArrowUnsignedIntWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowUnsignedIntReadData close(final int length) {
            final UInt4Vector vector = closeWithLength(length);
            return new ArrowUnsignedIntReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of unsigned {@link IntReadData}. */
    public static final class ArrowUnsignedIntReadData extends AbstractOffHeapArrowReadData<UInt4Vector>
        implements IntReadData {

        private ArrowUnsignedIntReadData(final UInt4Vector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowUnsignedIntReadData(final UInt4Vector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
        }

        /**
         * The underlying unsigned int value will be interpreted as signed int, such that values larger than
         * {@link Integer#MAX_VALUE} will be returned as negative numbers.
         */
        @Override
        public int getInt(final int index) {
            return m_vector.get(m_offset + index);
        }

        @Override
        public OffHeapArrowReadData slice(final int start, final int length) {
            return new ArrowUnsignedIntReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }
    }

    /** Implementation of {@link OffHeapArrowColumnDataFactory} for {@link OffHeapArrowUnsignedIntData} */
    @SuppressWarnings("javadoc")
    public static final class ArrowUnsignedIntDataFactory extends AbstractOffHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowUnsignedIntDataFactory} */
        public static final ArrowUnsignedIntDataFactory INSTANCE = new ArrowUnsignedIntDataFactory();

        private ArrowUnsignedIntDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.UINT4.getType());
        }

        @Override
        public ArrowUnsignedIntWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final UInt4Vector v = (UInt4Vector)vector;
            v.allocateNew(capacity);
            return new ArrowUnsignedIntWriteData(v);
        }

        @Override
        public ArrowUnsignedIntReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowUnsignedIntReadData((UInt4Vector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException("Cannot read ArrowUnsignedIntData with version " + version + ". Current version: "
                    + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return UInt4Vector.TYPE_WIDTH + 1; // +1 for validity
        }
    }
}
