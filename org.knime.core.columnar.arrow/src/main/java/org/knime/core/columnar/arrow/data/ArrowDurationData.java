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

import static org.apache.arrow.vector.NullCheckingForGet.NULL_CHECKING_ENABLED;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.DurationData.DurationReadData;
import org.knime.core.columnar.data.DurationData.DurationWriteData;

/**
 * Arrow implementation of {@link DurationWriteData} and {@link DurationReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowDurationData {

    private static final String CHILD_NAME_SECONDS = "seconds";

    private static final String CHILD_NAME_NANOS = "nanos";

    private static IntVector getNanosVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_NANOS, IntVector.class);
    }

    private static DurationVector getSecondsVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_SECONDS, DurationVector.class);
    }

    private ArrowDurationData() {
    }

    /** Arrow implementation of {@link DurationWriteData}. */
    public static final class ArrowDurationWriteData extends AbstractArrowWriteData<StructVector>
        implements DurationWriteData {

        // Package private for testing only
        final DurationVector m_secondsVector;

        // Package private for testing only
        final IntVector m_nanosVector;

        private ArrowDurationWriteData(final StructVector vector) {
            super(vector);
            m_secondsVector = getSecondsVector(vector);
            m_nanosVector = getNanosVector(vector);
        }

        private ArrowDurationWriteData(final StructVector vector, final int offset) {
            super(vector, offset);
            m_secondsVector = getSecondsVector(vector);
            m_nanosVector = getNanosVector(vector);
        }

        @Override
        public void setDuration(final int index, final Duration val) {
            m_vector.setIndexDefined(m_offset + index);
            m_secondsVector.set(m_offset + index, val.getSeconds());
            m_nanosVector.set(m_offset + index, val.getNano());
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowDurationWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return ArrowDurationData.sizeOf(m_vector, m_secondsVector, m_nanosVector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowDurationReadData close(final int length) {
            final StructVector vector = closeWithLength(length);
            return new ArrowDurationReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link DurationReadData}. */
    public static final class ArrowDurationReadData extends AbstractArrowReadData<StructVector>
        implements DurationReadData {

        // Package private for testing only
        final DurationVector m_secondsVector;

        // Package private for testing only
        final IntVector m_nanosVector;

        private ArrowDurationReadData(final StructVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
            m_secondsVector = getSecondsVector(vector);
            m_nanosVector = getNanosVector(vector);
        }

        private ArrowDurationReadData(final StructVector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
            m_secondsVector = getSecondsVector(vector);
            m_nanosVector = getNanosVector(vector);
        }

        @Override
        @SuppressWarnings("resource") // Data buffer is closed by vector
        public Duration getDuration(final int index) {
            // NB: DurationVector does not have a #get(int): long
            if (NULL_CHECKING_ENABLED && m_secondsVector.isSet(m_offset + index) == 0) {
                throw new IllegalStateException("Value at index is null");
            }
            final long seconds = DurationVector.get(m_secondsVector.getDataBuffer(), m_offset + index);
            final int nanos = m_nanosVector.get(m_offset + index);
            return Duration.ofSeconds(seconds, nanos);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowDurationReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowDurationData.sizeOf(m_vector, m_secondsVector, m_nanosVector);
        }
    }

    private static long sizeOf(final StructVector vector, final DurationVector secondsVector,
        final IntVector nanosVector) {
        return ArrowSizeUtils.sizeOfStruct(vector) + ArrowSizeUtils.sizeOfFixedWidth(secondsVector)
            + ArrowSizeUtils.sizeOfFixedWidth(nanosVector);
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowDurationData} */
    public static final class ArrowDurationDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowDurationDataFactory} */
        public static final ArrowDurationDataFactory INSTANCE = new ArrowDurationDataFactory();

        private ArrowDurationDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final Field secondsField = Field.nullable(CHILD_NAME_SECONDS, new ArrowType.Duration(TimeUnit.SECOND));
            final Field nanosField = Field.nullable(CHILD_NAME_NANOS, MinorType.INT.getType());
            return new Field(name, new FieldType(true, MinorType.STRUCT.getType(), null),
                Arrays.asList(secondsField, nanosField));
        }

        @Override
        public ArrowDurationWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final StructVector v = (StructVector)vector;
            v.setInitialCapacity(capacity);
            v.allocateNew();
            return new ArrowDurationWriteData(v);
        }

        @Override
        public ArrowDurationReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowDurationReadData((StructVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException(
                    "Cannot read ArrowDurationData with version " + version + ". Current version: " + m_version + ".");
            }
        }
    }
}
