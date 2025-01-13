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
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.offheap.OffHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.data.AbstractOffHeapArrowReadData.MissingValues;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.table.schema.DataSpec;

/**
 * Arrow implementation of {@link ByteWriteData} and {@link ByteReadData} representing unsigned values.
 *
 * Only to represent dictionary keys where we want to use the full range of 8 bits for non-negative numbers. There is no
 * {@link DataSpec} available for unsigned bytes, because Java does not support these as primitive types.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
final class OffHeapArrowUnsignedByteData {

    private OffHeapArrowUnsignedByteData() {
    }

    /** Arrow implementation of unsigned {@link ByteWriteData}. */
    public static final class ArrowUnsignedByteWriteData extends AbstractOffHeapArrowWriteData<UInt1Vector>
        implements ByteWriteData {

        private ArrowUnsignedByteWriteData(final UInt1Vector vector) {
            super(vector);
        }

        private ArrowUnsignedByteWriteData(final UInt1Vector vector, final int offset) {
            super(vector, offset);
        }

        /**
         * The signed byte value will be interpreted as unsigned byte, meaning all negative values will be mapped to
         * values larger than {@link Byte#MAX_VALUE}.
         */
        @Override
        public void setByte(final int index, final byte val) {
            m_vector.set(m_offset + index, val);
        }

        @Override
        public OffHeapArrowWriteData slice(final int start) {
            return new ArrowUnsignedByteWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowUnsignedByteReadData close(final int length) {
            final UInt1Vector vector = closeWithLength(length);
            return new ArrowUnsignedByteReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of unsigned {@link ByteReadData}. */
    public static final class ArrowUnsignedByteReadData extends AbstractOffHeapArrowReadData<UInt1Vector>
        implements ByteReadData {

        private ArrowUnsignedByteReadData(final UInt1Vector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowUnsignedByteReadData(final UInt1Vector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
        }

        /**
         * The underlying unsigned byte value will be interpreted as signed byte, such that values larger than
         * {@link Byte#MAX_VALUE} will be returned as negative numbers.
         */
        @Override
        public byte getByte(final int index) {
            return m_vector.get(m_offset + index);
        }

        @Override
        public OffHeapArrowReadData slice(final int start, final int length) {
            return new ArrowUnsignedByteReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }
    }

    /** Implementation of {@link OffHeapArrowColumnDataFactory} for {@link OffHeapArrowUnsignedByteData} */
    @SuppressWarnings("javadoc")
    public static final class ArrowUnsignedByteDataFactory extends AbstractOffHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowUnsignedByteDataFactory} */
        public static final ArrowUnsignedByteDataFactory INSTANCE = new ArrowUnsignedByteDataFactory();

        private ArrowUnsignedByteDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.UINT1.getType());
        }

        @Override
        public ArrowUnsignedByteWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final UInt1Vector v = (UInt1Vector)vector;
            v.allocateNew(capacity);
            return new ArrowUnsignedByteWriteData(v);
        }

        @Override
        public ArrowUnsignedByteReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowUnsignedByteReadData((UInt1Vector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException("Cannot read ArrowUnsignedByteData with version " + version
                    + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return UInt1Vector.TYPE_WIDTH + 1; // +1 for validity
        }
    }
}
