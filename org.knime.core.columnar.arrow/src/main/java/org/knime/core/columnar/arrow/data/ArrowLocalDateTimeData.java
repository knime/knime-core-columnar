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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeReadData;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeWriteData;

/**
 * Arrow implementation of {@link LocalDateTimeWriteData} and {@link LocalDateTimeReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowLocalDateTimeData {

    private static final String CHILD_NAME_EPOCH_DAY = "epochDay";

    private static final String CHILD_NAME_NANO_OF_DAY = "nanoOfDay";

    private static BigIntVector getEpochDayVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_EPOCH_DAY, BigIntVector.class);
    }

    private static TimeNanoVector getNanoOfDayVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_NANO_OF_DAY, TimeNanoVector.class);
    }

    private ArrowLocalDateTimeData() {
    }

    /** Arrow implementation of {@link LocalDateTimeWriteData}. */
    public static final class ArrowLocalDateTimeWriteData extends AbstractArrowWriteData<StructVector>
        implements LocalDateTimeWriteData {

        // Package private for testing only
        final BigIntVector m_epochDayVector;

        // Package private for testing only
        final TimeNanoVector m_nanoOfDayVector;

        private ArrowLocalDateTimeWriteData(final StructVector vector) {
            super(vector);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
        }

        private ArrowLocalDateTimeWriteData(final StructVector vector, final int offset) {
            super(vector, offset);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
        }

        @Override
        public void setLocalDateTime(final int index, final LocalDateTime val) {
            m_vector.setIndexDefined(m_offset + index);
            m_epochDayVector.set(m_offset + index, val.getLong(ChronoField.EPOCH_DAY));
            m_nanoOfDayVector.set(m_offset + index, val.getLong(ChronoField.NANO_OF_DAY));
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowLocalDateTimeWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return ArrowLocalDateTimeData.sizeOf(m_vector, m_epochDayVector, m_nanoOfDayVector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowLocalDateTimeReadData close(final int length) {
            final StructVector vector = closeWithLength(length);
            return new ArrowLocalDateTimeReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link LocalDateTimeReadData}. */
    public static final class ArrowLocalDateTimeReadData extends AbstractArrowReadData<StructVector>
        implements LocalDateTimeReadData {

        // Package private for testing only
        final BigIntVector m_epochDayVector;

        // Package private for testing only
        final TimeNanoVector m_nanoOfDayVector;

        private ArrowLocalDateTimeReadData(final StructVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
        }

        private ArrowLocalDateTimeReadData(final StructVector vector, final MissingValues missingValues,
            final int offset, final int length) {
            super(vector, missingValues, offset, length);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
        }

        @Override
        public LocalDateTime getLocalDateTime(final int index) {
            final long epochDay = m_epochDayVector.get(m_offset + index);
            final long nanoOfDay = m_nanoOfDayVector.get(m_offset + index);
            return LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay));
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowLocalDateTimeReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowLocalDateTimeData.sizeOf(m_vector, m_epochDayVector, m_nanoOfDayVector);
        }
    }

    private static long sizeOf(final StructVector vector, final BigIntVector epochDayVector,
        final TimeNanoVector nanoOfDayVector) {
        return ArrowSizeUtils.sizeOfStruct(vector) + ArrowSizeUtils.sizeOfFixedWidth(epochDayVector)
            + ArrowSizeUtils.sizeOfFixedWidth(nanoOfDayVector);
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowLocalDateTimeData} */
    public static final class ArrowLocalDateTimeDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowLocalDateTimeDataFactory} */
        public static final ArrowLocalDateTimeDataFactory INSTANCE = new ArrowLocalDateTimeDataFactory();

        private ArrowLocalDateTimeDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final Field epochDayField = Field.nullable(CHILD_NAME_EPOCH_DAY, MinorType.BIGINT.getType());
            final Field nanoOfDayField = Field.nullable(CHILD_NAME_NANO_OF_DAY, MinorType.TIMENANO.getType());
            return new Field(name, new FieldType(true, MinorType.STRUCT.getType(), null),
                Arrays.asList(epochDayField, nanoOfDayField));
        }

        @Override
        public ArrowLocalDateTimeWriteData createWrite(final FieldVector vector,
            final LongSupplier dictionaryIdSupplier, final BufferAllocator allocator, final int capacity) {
            final StructVector v = (StructVector)vector;
            v.setInitialCapacity(capacity);
            v.allocateNew();
            return new ArrowLocalDateTimeWriteData(v);
        }

        @Override
        public ArrowLocalDateTimeReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowLocalDateTimeReadData((StructVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException("Cannot read ArrowLocalDateTimeData with version " + version
                    + ". Current version: " + m_version + ".");
            }
        }
    }
}
