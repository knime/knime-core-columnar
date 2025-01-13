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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.offheap.OffHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.data.AbstractOffHeapArrowReadData.MissingValues;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;

/**
 * Arrow implementation of {@link DoubleWriteData} and {@link DoubleReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OffHeapArrowDoubleData {

    private OffHeapArrowDoubleData() {
    }

    /** Arrow implementation of {@link DoubleWriteData}. */
    public static final class ArrowDoubleWriteData extends AbstractOffHeapArrowWriteData<Float8Vector>
        implements DoubleWriteData {

        private ArrowDoubleWriteData(final Float8Vector vector) {
            super(vector);
        }

        private ArrowDoubleWriteData(final Float8Vector vector, final int offset) {
            super(vector, offset);
        }

        @Override
        public void setDouble(final int index, final double val) {
            m_vector.set(m_offset + index, val);
        }

        @Override
        public OffHeapArrowWriteData slice(final int start) {
            return new ArrowDoubleWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowDoubleReadData close(final int length) {
            final Float8Vector vector = closeWithLength(length);
            return new ArrowDoubleReadData(vector, MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link DoubleReadData}. */
    public static final class ArrowDoubleReadData extends AbstractOffHeapArrowReadData<Float8Vector>
        implements DoubleReadData {

        private ArrowDoubleReadData(final Float8Vector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowDoubleReadData(final Float8Vector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
        }

        @Override
        public double getDouble(final int index) {
            return m_vector.get(m_offset + index);
        }

        @Override
        public OffHeapArrowReadData slice(final int start, final int length) {
            return new ArrowDoubleReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }
    }

    /** Implementation of {@link OffHeapArrowColumnDataFactory} for {@link OffHeapArrowDoubleData} */
    public static final class ArrowDoubleDataFactory extends AbstractOffHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowDoubleDataFactory} */
        public static final ArrowDoubleDataFactory INSTANCE = new ArrowDoubleDataFactory();

        private ArrowDoubleDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.FLOAT8.getType());
        }

        @Override
        public ArrowDoubleWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final Float8Vector v = (Float8Vector)vector;
            v.allocateNew(capacity);
            return new ArrowDoubleWriteData(v);
        }

        @Override
        public ArrowDoubleReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowDoubleReadData((Float8Vector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException(
                    "Cannot read ArrowDoubleData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return Float8Vector.TYPE_WIDTH + 1; // +1 for validity
        }
    }
}
