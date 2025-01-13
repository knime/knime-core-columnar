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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.offheap.OffHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.data.AbstractOffHeapArrowReadData.MissingValues;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;

/**
 * Arrow implementation of {@link BooleanWriteData} and {@link BooleanReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OffHeapArrowBooleanData {

    private OffHeapArrowBooleanData() {
    }

    /** Arrow implementation of {@link BooleanWriteData}. */
    public static final class ArrowBooleanWriteData extends AbstractOffHeapArrowWriteData<BitVector>
        implements BooleanWriteData {

        private ArrowBooleanWriteData(final BitVector vector) {
            super(vector);
        }

        private ArrowBooleanWriteData(final BitVector vector, final int offset) {
            super(vector, offset);
        }

        @Override
        public void setBoolean(final int index, final boolean val) {
            m_vector.set(m_offset + index, val ? 1 : 0);
        }

        @Override
        public OffHeapArrowWriteData slice(final int start) {
            return new ArrowBooleanWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowBooleanReadData close(final int length) {
            final BitVector vector = closeWithLength(length);
            return new ArrowBooleanReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link BooleanReadData}. */
    public static final class ArrowBooleanReadData extends AbstractOffHeapArrowReadData<BitVector> implements BooleanReadData {

        private ArrowBooleanReadData(final BitVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowBooleanReadData(final BitVector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
        }

        @Override
        public boolean getBoolean(final int index) {
            return m_vector.get(m_offset + index) != 0;
        }

        @Override
        public OffHeapArrowReadData slice(final int start, final int length) {
            return new ArrowBooleanReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }
    }

    /** Implementation of {@link OffHeapArrowColumnDataFactory} for {@link OffHeapArrowBooleanData} */
    public static final class ArrowBooleanDataFactory extends AbstractOffHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowBooleanDataFactory} */
        public static final ArrowBooleanDataFactory INSTANCE = new ArrowBooleanDataFactory();

        private ArrowBooleanDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.BIT.getType());
        }

        @Override
        public ArrowBooleanWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final BitVector v = (BitVector)vector;
            v.allocateNew(capacity);
            return new ArrowBooleanWriteData(v);
        }

        @Override
        public ArrowBooleanReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowBooleanReadData((BitVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException(
                    "Cannot read ArrowBooleanData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return 1; // one bit validity, one bit value is less than a byte, but it's a good estimate
        }
    }
}
