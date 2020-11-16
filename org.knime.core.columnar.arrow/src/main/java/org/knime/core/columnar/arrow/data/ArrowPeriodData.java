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
import java.time.Period;
import java.util.Arrays;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.PeriodData.PeriodReadData;
import org.knime.core.columnar.data.PeriodData.PeriodWriteData;

/**
 * Arrow implementation of {@link PeriodWriteData} and {@link PeriodReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowPeriodData {

    private static final String CHILD_NAME_YEARS = "years";

    private static final String CHILD_NAME_MONTHS = "months";

    private static final String CHILD_NAME_DAYS = "days";

    private static IntVector getYearsVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_YEARS, IntVector.class);
    }

    private static IntVector getMonthsVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_MONTHS, IntVector.class);
    }

    private static IntVector getDaysVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_DAYS, IntVector.class);
    }

    private ArrowPeriodData() {
    }

    /** Arrow implementation of {@link PeriodWriteData}. */
    public static final class ArrowPeriodWriteData extends AbstractArrowWriteData<StructVector>
        implements PeriodWriteData {

        // Package private for testing only
        final IntVector m_yearsVector;

        // Package private for testing only
        final IntVector m_monthsVector;

        // Package private for testing only
        final IntVector m_daysVector;

        private ArrowPeriodWriteData(final StructVector vector) {
            super(vector);
            m_yearsVector = getYearsVector(vector);
            m_monthsVector = getMonthsVector(vector);
            m_daysVector = getDaysVector(vector);
        }

        private ArrowPeriodWriteData(final StructVector vector, final int offset) {
            super(vector, offset);
            m_yearsVector = getYearsVector(vector);
            m_monthsVector = getMonthsVector(vector);
            m_daysVector = getDaysVector(vector);
        }

        @Override
        public void setPeriod(final int index, final Period val) {
            m_vector.setIndexDefined(m_offset + index);
            m_yearsVector.set(m_offset + index, val.getYears());
            m_monthsVector.set(m_offset + index, val.getMonths());
            m_daysVector.set(m_offset + index, val.getDays());
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowPeriodWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return ArrowPeriodData.sizeOf(m_vector, m_yearsVector, m_monthsVector, m_daysVector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowPeriodReadData close(final int length) {
            final StructVector vector = closeWithLength(length);
            return new ArrowPeriodReadData(vector, MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link PeriodReadData}. */
    public static final class ArrowPeriodReadData extends AbstractArrowReadData<StructVector>
        implements PeriodReadData {

        // Package private for testing only
        final IntVector m_yearsVector;

        // Package private for testing only
        final IntVector m_monthsVector;

        // Package private for testing only
        final IntVector m_daysVector;

        private ArrowPeriodReadData(final StructVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
            m_yearsVector = getYearsVector(vector);
            m_monthsVector = getMonthsVector(vector);
            m_daysVector = getDaysVector(vector);
        }

        private ArrowPeriodReadData(final StructVector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
            m_yearsVector = getYearsVector(vector);
            m_monthsVector = getMonthsVector(vector);
            m_daysVector = getDaysVector(vector);
        }

        @Override
        public Period getPeriod(final int index) {
            final int years = m_yearsVector.get(m_offset + index);
            final int months = m_monthsVector.get(m_offset + index);
            final int days = m_daysVector.get(m_offset + index);
            return Period.of(years, months, days);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowPeriodReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowPeriodData.sizeOf(m_vector, m_yearsVector, m_monthsVector, m_daysVector);
        }
    }

    private static long sizeOf(final StructVector vector, final IntVector yearsVector, final IntVector monthsVector,
        final IntVector daysVector) {
        return ArrowSizeUtils.sizeOfStruct(vector) + ArrowSizeUtils.sizeOfFixedWidth(yearsVector)
            + ArrowSizeUtils.sizeOfFixedWidth(monthsVector) + ArrowSizeUtils.sizeOfFixedWidth(daysVector);
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowPeriodData} */
    public static final class ArrowPeriodDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowPeriodDataFactory} */
        public static final ArrowPeriodDataFactory INSTANCE = new ArrowPeriodDataFactory();

        private ArrowPeriodDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final Field yearsField = Field.nullable(CHILD_NAME_YEARS, MinorType.INT.getType());
            final Field monthsField = Field.nullable(CHILD_NAME_MONTHS, MinorType.INT.getType());
            final Field daysField = Field.nullable(CHILD_NAME_DAYS, MinorType.INT.getType());
            return new Field(name, new FieldType(true, MinorType.STRUCT.getType(), null),
                Arrays.asList(yearsField, monthsField, daysField));
        }

        @Override
        public ArrowPeriodWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final StructVector v = (StructVector)vector;
            v.setInitialCapacity(capacity);
            v.allocateNew();
            return new ArrowPeriodWriteData(v);
        }

        @Override
        public ArrowPeriodReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowPeriodReadData((StructVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException(
                    "Cannot read ArrowPeriodData with version " + version + ". Current version: " + m_version + ".");
            }
        }
    }
}
