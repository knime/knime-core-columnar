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
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;

/**
 * Arrow implementation of {@link LongWriteData} and {@link LongReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowLongData {

    private ArrowLongData() {
    }

    /** Arrow implementation of {@link LongWriteData}. */
    public static final class ArrowLongWriteData extends AbstractArrowWriteData<BigIntVector> implements LongWriteData {

        private ArrowLongWriteData(final BigIntVector vector) {
            super(vector);
        }

        private ArrowLongWriteData(final BigIntVector vector, final int offset) {
            super(vector, offset);
        }

        @Override
        public void setLong(final int index, final long val) {
            m_vector.set(m_offset + index, val);
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowLongWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowLongReadData close(final int length) {
            final BigIntVector vector = closeWithLength(length);
            return new ArrowLongReadData(vector, MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /**
     * For reading longs from a DurationVector. This is a class introduced for backwards compatibility with data stored
     * prior to the removal of the Date&Time dataspecs (i.e. anything before KNIME Analytics Platform 4.5).
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class ArrowDurationLongReadData extends AbstractArrowReadData<DurationVector> implements LongReadData {

        private ArrowDurationLongReadData(final DurationVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowDurationLongReadData(final DurationVector vector, final MissingValues missingValues,
            final int offset, final int length) {
            super(vector, missingValues, offset, length);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowDurationLongReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        public long getLong(final int index) {
            // NB: DurationVector does not have a #get(int): long
            @SuppressWarnings("resource") // not ours to close
            final long value = DurationVector.get(m_vector.getDataBuffer(), m_offset + index);
            return value;
        }

    }

    /**
     * For reading longs from a TimeNanoVector. This is a class introduced for backwards compatibility with data stored
     * prior to the removal of the Date&Time dataspecs (i.e. anything before KNIME Analytics Platform 4.5).
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class ArrowTimeNanoLongReadData extends AbstractArrowReadData<TimeNanoVector> implements LongReadData {

        private ArrowTimeNanoLongReadData(final TimeNanoVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowTimeNanoLongReadData(final TimeNanoVector vector, final MissingValues missingValues,
            final int offset, final int length) {
            super(vector, missingValues, offset, length);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowTimeNanoLongReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long getLong(final int index) {
            return m_vector.get(m_offset + index);
        }

    }

    /** Arrow implementation of {@link LongReadData}. */
    public static final class ArrowLongReadData extends AbstractArrowReadData<BigIntVector> implements LongReadData {

        private ArrowLongReadData(final BigIntVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowLongReadData(final BigIntVector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
        }

        @Override
        public long getLong(final int index) {
            return m_vector.get(m_offset + index);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowLongReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowLongData} */
    public static final class ArrowLongDataFactory extends AbstractArrowColumnDataFactory {

        /**
         * Also supports reading longs from DurationVectors and TimeNanoVectors. This is necessary for backwards
         * compatibility of date&time types which now simply use BigIntVector.
         */
        private static final ArrowColumnDataFactoryVersion V0 = ArrowColumnDataFactoryVersion.version(0);

        /** Singleton instance of {@link ArrowLongDataFactory} */
        public static final ArrowLongDataFactory INSTANCE = new ArrowLongDataFactory();

        private ArrowLongDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(1));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.BIGINT.getType());
        }

        @Override
        public ArrowLongWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final BigIntVector v = (BigIntVector)vector;
            v.allocateNew(capacity);
            return new ArrowLongWriteData(v);
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            final var missingValues = MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount());
            if (V0.equals(version)) {
                if (vector instanceof DurationVector) {
                    return new ArrowDurationLongReadData((DurationVector)vector, missingValues);
                } else if (vector instanceof TimeNanoVector) {
                    return new ArrowTimeNanoLongReadData((TimeNanoVector)vector, missingValues);
                } else {
                    return new ArrowLongReadData((BigIntVector)vector,
                        missingValues);
                }
            } else if (m_version.equals(version)) {
                return new ArrowLongReadData((BigIntVector)vector, missingValues);
            } else {
                throw new IOException(
                    "Cannot read ArrowLongData with version " + version + ". Current version: " + m_version + ".");
            }
        }
    }
}
