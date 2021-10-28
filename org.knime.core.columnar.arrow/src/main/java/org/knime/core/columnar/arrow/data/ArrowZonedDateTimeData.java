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
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeReadData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeWriteData;

/**
 * Arrow implementation of {@link ZonedDateTimeWriteData} and {@link ZonedDateTimeReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowZonedDateTimeData {

    private static final int INIT_DICT_SIZE = 8;

    private static final String CHILD_NAME_EPOCH_DAY = "epochDay";

    private static final String CHILD_NAME_NANO_OF_DAY = "nanoOfDay";

    private static final String CHILD_NAME_ZONE_OFFSET = "zoneOffset";

    private static final String CHILD_NAME_ZONE_ID = "zoneId";

    private static BigIntVector getEpochDayVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_EPOCH_DAY, BigIntVector.class);
    }

    private static TimeNanoVector getNanoOfDayVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_NANO_OF_DAY, TimeNanoVector.class);
    }

    private static IntVector getZoneOffsetVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_ZONE_OFFSET, IntVector.class);
    }

    private static VarCharVector getZoneIdVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_ZONE_ID, VarCharVector.class);
    }

    private static long sizeOf(final StructVector vector, final BigIntVector epochDayVector,
        final TimeNanoVector nanoOfDayVector, final IntVector zoneOffsetVector, final VarCharVector zoneIdVector) {
        return ArrowSizeUtils.sizeOfStruct(vector) //
            + ArrowSizeUtils.sizeOfFixedWidth(epochDayVector) //
            + ArrowSizeUtils.sizeOfFixedWidth(nanoOfDayVector) //
            + ArrowSizeUtils.sizeOfFixedWidth(zoneOffsetVector) //
            + ArrowSizeUtils.sizeOfVariableWidth(zoneIdVector);
    }

    private ArrowZonedDateTimeData() {
    }

    /** Arrow implementation of {@link ZonedDateTimeWriteData}. */
    public static final class ArrowZonedDateTimeWriteData extends AbstractArrowWriteData<StructVector>
        implements ZonedDateTimeWriteData {

        // Package private for testing only
        final BigIntVector m_epochDayVector;

        // Package private for testing only
        final TimeNanoVector m_nanoOfDayVector;

        // Package private for testing only
        final IntVector m_zoneOffsetVector;

        // Package private for testing only
        final VarCharVector m_zoneIdVector;

        private final StringEncoder m_encoder;

        private ArrowZonedDateTimeWriteData(final StructVector vector) {
            this(vector, 0);
        }

        private ArrowZonedDateTimeWriteData(final StructVector vector, final int offset) {
            super(vector, offset);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
            m_zoneOffsetVector = getZoneOffsetVector(vector);
            m_zoneIdVector = getZoneIdVector(vector);
            m_encoder = new StringEncoder();
        }

        @Override
        public void setZonedDateTime(final int index, final ZonedDateTime val) {
            m_vector.setIndexDefined(m_offset + index);
            m_epochDayVector.set(m_offset + index, val.getLong(ChronoField.EPOCH_DAY));
            m_nanoOfDayVector.set(m_offset + index, val.getLong(ChronoField.NANO_OF_DAY));
            m_zoneOffsetVector.set(m_offset + index, val.getOffset().get(ChronoField.OFFSET_SECONDS));
            final ByteBuffer encodedZoneId = m_encoder.encode(val.getZone().getId());
            m_zoneIdVector.setSafe(m_offset + index, encodedZoneId, 0, encodedZoneId.limit());
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowZonedDateTimeWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return ArrowZonedDateTimeData.sizeOf(m_vector, m_epochDayVector, m_nanoOfDayVector, m_zoneOffsetVector,
                m_zoneIdVector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowZonedDateTimeReadData close(final int length) {
            final StructVector vector = closeWithLength(length);
            return new ArrowZonedDateTimeReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link ZonedDateTimeReadData}. */
    public static final class ArrowZonedDateTimeReadData extends AbstractArrowReadData<StructVector>
        implements ZonedDateTimeReadData {

        // Package private for testing only
        final BigIntVector m_epochDayVector;

        // Package private for testing only
        final TimeNanoVector m_nanoOfDayVector;

        // Package private for testing only
        final IntVector m_zoneOffsetVector;

        // Package private for testing only
        final VarCharVector m_zoneIdVector;

        private final StringEncoder m_decoder;

        private ArrowZonedDateTimeReadData(final StructVector vector, final MissingValues missingValues) {
            this(vector, missingValues, 0, vector.getValueCount());
        }

        private ArrowZonedDateTimeReadData(final StructVector vector, final MissingValues missingValues,
            final int offset, final int length) {
            super(vector, missingValues, offset, length);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
            m_zoneOffsetVector = getZoneOffsetVector(vector);
            m_zoneIdVector = getZoneIdVector(vector);
            m_decoder = new StringEncoder();
        }

        @Override
        public ZonedDateTime getZonedDateTime(final int index) {
            final long epochDay = m_epochDayVector.get(m_offset + index);
            final long nanoOfDay = m_nanoOfDayVector.get(m_offset + index);
            final int zoneOffset = m_zoneOffsetVector.get(m_offset + index);
            final ZoneId zoneId = ZoneId.of(m_decoder.decode(m_zoneIdVector.get(m_offset + index)));
            return ZonedDateTime.ofInstant(
                LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay)),
                ZoneOffset.ofTotalSeconds(zoneOffset), zoneId);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowZonedDateTimeReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowZonedDateTimeData.sizeOf(m_vector, m_epochDayVector, m_nanoOfDayVector, m_zoneOffsetVector,
                m_zoneIdVector);
        }
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowZonedDateTimeData} */
    public static final class ArrowZonedDateTimeDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowZonedDateTimeDataFactory} */
        public static final ArrowZonedDateTimeDataFactory INSTANCE = new ArrowZonedDateTimeDataFactory();

        private static final ArrowColumnDataFactoryVersion V0 = ArrowColumnDataFactoryVersion.version(0);

        private ArrowZonedDateTimeDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(1));
        }

        /**
         * Checks if the provided field corresponds to a ZonedDateTimeField.
         *
         * @param field to check
         * @return true if the provided field is a ZonedDateTimeField
         */
        public static boolean isZonedDateTimeField(final Field field) {
            return field.getType().equals(MinorType.STRUCT.getType())//
                    && checkChildren(field.getChildren());
        }

        private static boolean checkChildren(final List<Field> children) {
            return children.size() == 4//NOSONAR
                    && isChildField(children.get(0), CHILD_NAME_EPOCH_DAY, MinorType.BIGINT)//
                    && isChildField(children.get(1), CHILD_NAME_NANO_OF_DAY, MinorType.TIMENANO)//
                    && isChildField(children.get(2), CHILD_NAME_ZONE_OFFSET, MinorType.INT)//
                    && (isChildField(children.get(3), CHILD_NAME_ZONE_ID, MinorType.VARCHAR)
                        || isChildField(children.get(3), CHILD_NAME_ZONE_ID, MinorType.LARGEVARBINARY));
        }

        private static boolean isChildField(final Field field, final String name, final MinorType type) {
            return field.isNullable()//
                    && field.getType().equals(type.getType())//
                    && field.getName().equals(name);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final var epochDayField = Field.nullable(CHILD_NAME_EPOCH_DAY, MinorType.BIGINT.getType());
            final var nanoOfDayField = Field.nullable(CHILD_NAME_NANO_OF_DAY, MinorType.TIMENANO.getType());
            final var zoneOffset = Field.nullable(CHILD_NAME_ZONE_OFFSET, MinorType.INT.getType());
            final var zoneId = Field.nullable(CHILD_NAME_ZONE_ID, MinorType.VARCHAR.getType());
            return new Field(name, new FieldType(true, MinorType.STRUCT.getType(), null),
                Arrays.asList(epochDayField, nanoOfDayField, zoneOffset, zoneId));
        }

        @Override
        public ArrowZonedDateTimeWriteData createWrite(final FieldVector vector,
            final LongSupplier dictionaryIdSupplier, final BufferAllocator allocator, final int capacity) {
            final StructVector v = (StructVector)vector;
            v.setInitialCapacity(capacity);
            v.allocateNew();
            return new ArrowZonedDateTimeWriteData(v);
        }

        @Override
        @SuppressWarnings("resource") // Dictionary vector closed by data object
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                // Current version
                return new ArrowZonedDateTimeReadData((StructVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else if (V0.equals(version)) {
                // VO: Legacy handling with dictionary for ZoneId
                final StructVector v = (StructVector)vector;
                final long dictId = getZoneIdVectorV0(v).getField().getFieldType().getDictionary().getId();
                final LargeVarBinaryVector dict = (LargeVarBinaryVector)provider.lookup(dictId).getVector();
                return new ArrowZonedDateTimeReadDataV0(v,
                    MissingValues.forNullCount(nullCount.getNullCount(), v.getValueCount()), dict);
            } else {
                throw new IOException("Cannot read ArrowZonedDateTimeData with version " + version
                    + ". Current version: " + m_version + ".");
            }
        }
    }

    // =======================================================================
    // LEGACY READ SUPPORT
    // =======================================================================

    private static IntVector getZoneIdVectorV0(final StructVector vector) {
        return vector.getChild(CHILD_NAME_ZONE_ID, IntVector.class);
    }

    /** Legacy implementation of {@link ZonedDateTimeReadData}. */
    static final class ArrowZonedDateTimeReadDataV0 extends AbstractArrowReadData<StructVector>
        implements ZonedDateTimeReadData {

        // Package private for testing only
        final BigIntVector m_epochDayVector;

        // Package private for testing only
        final TimeNanoVector m_nanoOfDayVector;

        // Package private for testing only
        final IntVector m_zoneOffsetVector;

        // Package private for testing only
        final IntVector m_zoneIdVector;

        private final InMemoryDictEncoding<ZoneId> m_dict;

        private ArrowZonedDateTimeReadDataV0(final StructVector vector, final MissingValues missingValues,
            final LargeVarBinaryVector dict) {
            super(vector, missingValues);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
            m_zoneOffsetVector = getZoneOffsetVector(vector);
            m_zoneIdVector = getZoneIdVectorV0(vector);
            m_dict = getInMemoryDict(dict);
        }

        private ArrowZonedDateTimeReadDataV0(final StructVector vector, final MissingValues missingValues,
            final int offset, final int length, final InMemoryDictEncoding<ZoneId> dict) {
            super(vector, missingValues, offset, length);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
            m_zoneOffsetVector = getZoneOffsetVector(vector);
            m_zoneIdVector = getZoneIdVectorV0(vector);
            m_dict = dict;
        }

        @Override
        public ZonedDateTime getZonedDateTime(final int index) {
            final long epochDay = m_epochDayVector.get(m_offset + index);
            final long nanoOfDay = m_nanoOfDayVector.get(m_offset + index);
            final int zoneOffset = m_zoneOffsetVector.get(m_offset + index);
            final ZoneId zoneId = m_dict.get(m_zoneIdVector.get(m_offset + index));
            return ZonedDateTime.ofInstant(
                LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay)),
                ZoneOffset.ofTotalSeconds(zoneOffset), zoneId);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowZonedDateTimeReadDataV0(m_vector, m_missingValues, m_offset + start, length, m_dict);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfStruct(m_vector) //
                + ArrowSizeUtils.sizeOfFixedWidth(m_epochDayVector) //
                + ArrowSizeUtils.sizeOfFixedWidth(m_nanoOfDayVector) //
                + ArrowSizeUtils.sizeOfFixedWidth(m_zoneOffsetVector) //
                + ArrowSizeUtils.sizeOfFixedWidth(m_zoneIdVector) //
                + m_dict.sizeOf();
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_dict.closeVector();
        }

        private static InMemoryDictEncoding<ZoneId> getInMemoryDict(final LargeVarBinaryVector dict) {
            return new InMemoryDictEncoding<>(dict, INIT_DICT_SIZE, (output, id) -> output.writeUTF(id.getId()),
                input -> ZoneId.of(input.readUTF()));
        }
    }
}
