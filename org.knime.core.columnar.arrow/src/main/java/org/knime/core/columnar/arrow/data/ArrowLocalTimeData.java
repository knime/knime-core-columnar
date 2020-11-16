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
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeReadData;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeWriteData;

/**
 * Arrow implementation of {@link LocalTimeWriteData} and {@link LocalTimeReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowLocalTimeData {

    private ArrowLocalTimeData() {
    }

    /** Arrow implementation of {@link LocalTimeWriteData}. */
    public static final class ArrowLocalTimeWriteData extends AbstractArrowWriteData<TimeNanoVector>
        implements LocalTimeWriteData {

        private ArrowLocalTimeWriteData(final TimeNanoVector vector) {
            super(vector);
        }

        private ArrowLocalTimeWriteData(final TimeNanoVector vector, final int offset) {
            super(vector, offset);
        }

        @Override
        public void setLocalTime(final int index, final LocalTime val) {
            m_vector.set(m_offset + index, val.getLong(ChronoField.NANO_OF_DAY));
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowLocalTimeWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowLocalTimeReadData close(final int length) {
            final TimeNanoVector vector = closeWithLength(length);
            return new ArrowLocalTimeReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link LocalTimeReadData}. */
    public static final class ArrowLocalTimeReadData extends AbstractArrowReadData<TimeNanoVector>
        implements LocalTimeReadData {

        private ArrowLocalTimeReadData(final TimeNanoVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowLocalTimeReadData(final TimeNanoVector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
        }

        @Override
        public LocalTime getLocalTime(final int index) {
            return LocalTime.ofNanoOfDay(m_vector.get(m_offset + index));
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowLocalTimeReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowLocalTimeData} */
    public static final class ArrowLocalTimeDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowLocalTimeDataFactory} */
        public static final ArrowLocalTimeDataFactory INSTANCE = new ArrowLocalTimeDataFactory();

        private ArrowLocalTimeDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.TIMENANO.getType());
        }

        @Override
        public ArrowLocalTimeWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final TimeNanoVector v = (TimeNanoVector)vector;
            v.allocateNew(capacity);
            return new ArrowLocalTimeWriteData(v);
        }

        @Override
        public ArrowLocalTimeReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowLocalTimeReadData((TimeNanoVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException(
                    "Cannot read ArrowLocalTimeData with version " + version + ". Current version: " + m_version + ".");
            }
        }
    }
}
