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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.SingletonDictionaryProvider;
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ObjectData.ObjectDataSerializer;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeReadData;
import org.knime.core.columnar.data.ZonedDateTimeData.ZonedDateTimeWriteData;

/**
 * Arrow implementation of {@link ZonedDateTimeWriteData} and {@link ZonedDateTimeReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowZonedDateTimeData {

    private static final ZoneIdSerializer ZONE_ID_SERIALIZER = new ZoneIdSerializer();

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

    private static IntVector getZoneIdVector(final StructVector vector) {
        return vector.getChild(CHILD_NAME_ZONE_ID, IntVector.class);
    }

    private static InMemoryDictEncoding<ZoneId> getInMemoryDict(final LargeVarBinaryVector dict) {
        return new InMemoryDictEncoding<>(dict, new ArrowBufIO<>(dict, ZONE_ID_SERIALIZER), INIT_DICT_SIZE);
    }

    private ArrowZonedDateTimeData() {
    }

    private static final class ZoneIdSerializer implements ObjectDataSerializer<ZoneId> {
        @Override
        public void serialize(final ZoneId obj, final DataOutput output) throws IOException {
            output.writeUTF(obj.getId());
        }

        @Override
        public ZoneId deserialize(final DataInput input) throws IOException {
            return ZoneId.of(input.readUTF());
        }
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
        final IntVector m_zoneIdVector;

        private final InMemoryDictEncoding<ZoneId> m_dict;

        private ArrowZonedDateTimeWriteData(final StructVector vector, final LargeVarBinaryVector dict) {
            super(vector);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
            m_zoneOffsetVector = getZoneOffsetVector(vector);
            m_zoneIdVector = getZoneIdVector(vector);
            m_dict = getInMemoryDict(dict);
        }

        private ArrowZonedDateTimeWriteData(final StructVector vector, final int offset,
            final InMemoryDictEncoding<ZoneId> dict) {
            super(vector, offset);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
            m_zoneOffsetVector = getZoneOffsetVector(vector);
            m_zoneIdVector = getZoneIdVector(vector);
            m_dict = dict;
        }

        @Override
        public void setZonedDateTime(final int index, final ZonedDateTime val) {
            m_vector.setIndexDefined(m_offset + index);
            m_epochDayVector.set(m_offset + index, val.getLong(ChronoField.EPOCH_DAY));
            m_nanoOfDayVector.set(m_offset + index, val.getLong(ChronoField.NANO_OF_DAY));
            m_zoneOffsetVector.set(m_offset + index, val.getOffset().get(ChronoField.OFFSET_SECONDS));
            m_zoneIdVector.set(m_offset + index, m_dict.set(val.getZone()));
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowZonedDateTimeWriteData(m_vector, m_offset + start, m_dict);
        }

        @Override
        public long sizeOf() {
            return ArrowZonedDateTimeData.sizeOf(m_vector, m_epochDayVector, m_nanoOfDayVector, m_zoneOffsetVector,
                m_zoneIdVector) + m_dict.sizeOf();
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowZonedDateTimeReadData close(final int length) {
            final StructVector vector = closeWithLength(length);
            return new ArrowZonedDateTimeReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length), m_dict);
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_dict.closeVector();
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
        final IntVector m_zoneIdVector;

        private final InMemoryDictEncoding<ZoneId> m_dict;

        private ArrowZonedDateTimeReadData(final StructVector vector, final MissingValues missingValues,
            final LargeVarBinaryVector dict) {
            this(vector, missingValues, getInMemoryDict(dict));
        }

        private ArrowZonedDateTimeReadData(final StructVector vector, final MissingValues missingValues,
            final InMemoryDictEncoding<ZoneId> dict) {
            super(vector, missingValues);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
            m_zoneOffsetVector = getZoneOffsetVector(vector);
            m_zoneIdVector = getZoneIdVector(vector);
            m_dict = dict;
        }

        private ArrowZonedDateTimeReadData(final StructVector vector, final MissingValues missingValues,
            final int offset, final int length, final InMemoryDictEncoding<ZoneId> dict) {
            super(vector, missingValues, offset, length);
            m_epochDayVector = getEpochDayVector(vector);
            m_nanoOfDayVector = getNanoOfDayVector(vector);
            m_zoneOffsetVector = getZoneOffsetVector(vector);
            m_zoneIdVector = getZoneIdVector(vector);
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
            return new ArrowZonedDateTimeReadData(m_vector, m_missingValues, m_offset + start, length, m_dict);
        }

        @Override
        public long sizeOf() {
            return ArrowZonedDateTimeData.sizeOf(m_vector, m_epochDayVector, m_nanoOfDayVector, m_zoneOffsetVector,
                m_zoneIdVector) + m_dict.sizeOf();
        }

        private LargeVarBinaryVector getDictionary() {
            return m_dict.getDictionaryVector();
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_dict.closeVector();
        }
    }

    private static long sizeOf(final StructVector vector, final BigIntVector epochDayVector,
        final TimeNanoVector nanoOfDayVector, final IntVector zoneOffsetVector, final IntVector zoneIdVector) {
        return ArrowSizeUtils.sizeOfStruct(vector) + ArrowSizeUtils.sizeOfFixedWidth(epochDayVector)
            + ArrowSizeUtils.sizeOfFixedWidth(nanoOfDayVector) + ArrowSizeUtils.sizeOfFixedWidth(zoneOffsetVector)
            + ArrowSizeUtils.sizeOfFixedWidth(zoneIdVector);
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowZonedDateTimeData} */
    public static final class ArrowZonedDateTimeDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowZonedDateTimeDataFactory} */
        public static final ArrowZonedDateTimeDataFactory INSTANCE = new ArrowZonedDateTimeDataFactory();

        private ArrowZonedDateTimeDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final Field epochDayField = Field.nullable(CHILD_NAME_EPOCH_DAY, MinorType.BIGINT.getType());
            final Field nanoOfDayField = Field.nullable(CHILD_NAME_NANO_OF_DAY, MinorType.TIMENANO.getType());
            final Field zoneOffset = Field.nullable(CHILD_NAME_ZONE_OFFSET, MinorType.INT.getType());
            final DictionaryEncoding zoneIdDict = new DictionaryEncoding(dictionaryIdSupplier.getAsLong(), false, null);
            final Field zoneId =
                new Field(CHILD_NAME_ZONE_ID, new FieldType(true, MinorType.INT.getType(), zoneIdDict), null);
            return new Field(name, new FieldType(true, MinorType.STRUCT.getType(), null),
                Arrays.asList(epochDayField, nanoOfDayField, zoneOffset, zoneId));
        }

        @Override
        @SuppressWarnings("resource") // Vector closed by data object
        public ArrowZonedDateTimeWriteData createWrite(final FieldVector vector,
            final LongSupplier dictionaryIdSupplier, final BufferAllocator allocator, final int capacity) {
            // Remove the dictionary id for this encoding from the supplier
            dictionaryIdSupplier.getAsLong();
            final LargeVarBinaryVector dict = new LargeVarBinaryVector("Dictionary", allocator);
            final StructVector v = (StructVector)vector;
            v.setInitialCapacity(capacity);
            v.allocateNew();
            return new ArrowZonedDateTimeWriteData(v, dict);
        }

        @Override
        @SuppressWarnings("resource") // Dictionary vector closed by data object
        public ArrowZonedDateTimeReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                final StructVector v = (StructVector)vector;
                final long dictId = getZoneIdVector(v).getField().getFieldType().getDictionary().getId();
                final LargeVarBinaryVector dict = (LargeVarBinaryVector)provider.lookup(dictId).getVector();
                return new ArrowZonedDateTimeReadData(v,
                    MissingValues.forNullCount(nullCount.getNullCount(), v.getValueCount()), dict);
            } else {
                throw new IOException("Cannot read ArrowZonedDateTimeData with version " + version
                    + ". Current version: " + m_version + ".");
            }
        }

        @Override
        @SuppressWarnings("resource") // Dictionary vector closed by data object
        public DictionaryProvider getDictionaries(final ColumnReadData data) {
            final ArrowZonedDateTimeReadData objData = (ArrowZonedDateTimeReadData)data;
            final LargeVarBinaryVector vector = objData.getDictionary();
            final Dictionary dictionary = new Dictionary(vector, objData.m_zoneIdVector.getField().getDictionary());
            return new SingletonDictionaryProvider(dictionary);
        }
    }
}
