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
 *  aInt with this program; if not, see <http://www.gnu.org/licenses>.
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
 *
 * History
 *   8 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;

import org.junit.Test;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.ObjectData.GenericObjectDataSpec;
import org.knime.core.columnar.data.ObjectData.ObjectDataSerializer;
import org.knime.core.columnar.testing.data.TestDurationData;
import org.knime.core.columnar.testing.data.TestDurationData.TestDurationDataFactory;
import org.knime.core.columnar.testing.data.TestGenericObjectData;
import org.knime.core.columnar.testing.data.TestGenericObjectData.TestGenericObjectDataFactory;
import org.knime.core.columnar.testing.data.TestLocalDateData;
import org.knime.core.columnar.testing.data.TestLocalDateData.TestLocalDateDataFactory;
import org.knime.core.columnar.testing.data.TestLocalDateTimeData;
import org.knime.core.columnar.testing.data.TestLocalDateTimeData.TestLocalDateTimeDataFactory;
import org.knime.core.columnar.testing.data.TestLocalTimeData;
import org.knime.core.columnar.testing.data.TestLocalTimeData.TestLocalTimeDataFactory;
import org.knime.core.columnar.testing.data.TestPeriodData;
import org.knime.core.columnar.testing.data.TestPeriodData.TestPeriodDataFactory;
import org.knime.core.columnar.testing.data.TestStringData;
import org.knime.core.columnar.testing.data.TestStringData.TestStringDataFactory;
import org.knime.core.columnar.testing.data.TestZonedDateTimeData;
import org.knime.core.columnar.testing.data.TestZonedDateTimeData.TestZonedDateTimeDataFactory;
import org.knime.core.data.v2.access.ObjectAccess.DurationAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.GenericObjectAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.LocalDateAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.LocalDateTimeAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.LocalTimeAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.ObjectReadAccess;
import org.knime.core.data.v2.access.ObjectAccess.ObjectSerializer;
import org.knime.core.data.v2.access.ObjectAccess.ObjectWriteAccess;
import org.knime.core.data.v2.access.ObjectAccess.PeriodAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.StringAccessSpec;
import org.knime.core.data.v2.access.ObjectAccess.ZonedDateTimeAccessSpec;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarObjectAccessFactoryTest {

    private static final class IntDataInputOutput implements DataInput, DataOutput {

        private Integer m_value;

        @Override
        public void write(final int b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(final byte[] b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(final byte[] b, final int off, final int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeBoolean(final boolean v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeByte(final int v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeShort(final int v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeChar(final int v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeInt(final int v) {
            m_value = Integer.valueOf(v);
        }

        @Override
        public void writeLong(final long v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeFloat(final float v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeDouble(final double v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeBytes(final String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeChars(final String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeUTF(final String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(final byte[] b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(final byte[] b, final int off, final int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int skipBytes(final int n) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean readBoolean() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte readByte() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readUnsignedByte() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short readShort() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readUnsignedShort() {
            throw new UnsupportedOperationException();
        }

        @Override
        public char readChar() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readInt() {
            return m_value.intValue();
        }

        @Override
        public long readLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float readFloat() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double readDouble() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String readLine() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String readUTF() {
            throw new UnsupportedOperationException();
        }

    }

    @Test
    public void testGenericAccess() throws IOException {
        final ObjectSerializer<Object> serializer = new ObjectSerializer<Object>() {
            @Override
            public Object deserialize(final DataInput access) throws IOException {
                return Integer.valueOf(access.readInt());
            }

            @Override
            public void serialize(final Object object, final DataOutput access) throws IOException {
                access.writeInt(((Integer)object).intValue());
            }
        };

        final GenericObjectAccessSpec<Object> spec = new GenericObjectAccessSpec<>(serializer);
        @SuppressWarnings("unchecked")
        final ColumnarObjectAccessFactory<Object> factory =
            (ColumnarObjectAccessFactory<Object>)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        final TestGenericObjectData data = TestGenericObjectDataFactory.INSTANCE.createWriteData(1);

        final ObjectWriteAccess<Object> writeAccess = factory.createWriteAccess(data, () -> 0);
        final ObjectReadAccess<Object> readAccess = factory.createReadAccess(data, () -> 0);

        final Object value = Integer.valueOf(42);
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());

        writeAccess.setObject(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getObject());

        @SuppressWarnings("unchecked")
        final ObjectDataSerializer<Object> dataSerializer =
            ((GenericObjectDataSpec<Object>)factory.getColumnDataSpec()).getSerializer();
        final IntDataInputOutput io = new IntDataInputOutput();
        dataSerializer.serialize(value, io);
        assertEquals(value, dataSerializer.deserialize(io));

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());
    }

    @Test
    public void testStringAccess() {
        final StringAccessSpec spec = StringAccessSpec.INSTANCE;
        @SuppressWarnings("unchecked")
        final ColumnarObjectAccessFactory<String> factory =
            (ColumnarObjectAccessFactory<String>)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.stringSpec(), factory.getColumnDataSpec());
        final TestStringData data = TestStringDataFactory.INSTANCE.createWriteData(1);

        final ObjectWriteAccess<String> writeAccess = factory.createWriteAccess(data, () -> 0);
        final ObjectReadAccess<String> readAccess = factory.createReadAccess(data, () -> 0);

        final String value = "test";
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());

        writeAccess.setObject(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getObject());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());
    }

    @Test
    public void testLocalDateAccess() {
        final LocalDateAccessSpec spec = LocalDateAccessSpec.INSTANCE;
        @SuppressWarnings("unchecked")
        final ColumnarObjectAccessFactory<LocalDate> factory =
            (ColumnarObjectAccessFactory<LocalDate>)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.localDateSpec(), factory.getColumnDataSpec());
        final TestLocalDateData data = TestLocalDateDataFactory.INSTANCE.createWriteData(1);

        final ObjectWriteAccess<LocalDate> writeAccess = factory.createWriteAccess(data, () -> 0);
        final ObjectReadAccess<LocalDate> readAccess = factory.createReadAccess(data, () -> 0);

        final LocalDate value = LocalDate.now();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());

        writeAccess.setObject(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getObject());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());
    }

    @Test
    public void testLocalTimeAccess() {
        final LocalTimeAccessSpec spec = LocalTimeAccessSpec.INSTANCE;
        @SuppressWarnings("unchecked")
        final ColumnarObjectAccessFactory<LocalTime> factory =
            (ColumnarObjectAccessFactory<LocalTime>)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.localTimeSpec(), factory.getColumnDataSpec());
        final TestLocalTimeData data = TestLocalTimeDataFactory.INSTANCE.createWriteData(1);

        final ObjectWriteAccess<LocalTime> writeAccess = factory.createWriteAccess(data, () -> 0);
        final ObjectReadAccess<LocalTime> readAccess = factory.createReadAccess(data, () -> 0);

        final LocalTime value = LocalTime.now();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());

        writeAccess.setObject(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getObject());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());
    }

    @Test
    public void testLocalDateTimeAccess() {
        final LocalDateTimeAccessSpec spec = LocalDateTimeAccessSpec.INSTANCE;
        @SuppressWarnings("unchecked")
        final ColumnarObjectAccessFactory<LocalDateTime> factory =
            (ColumnarObjectAccessFactory<LocalDateTime>)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.localDateTimeSpec(), factory.getColumnDataSpec());
        final TestLocalDateTimeData data = TestLocalDateTimeDataFactory.INSTANCE.createWriteData(1);

        final ObjectWriteAccess<LocalDateTime> writeAccess = factory.createWriteAccess(data, () -> 0);
        final ObjectReadAccess<LocalDateTime> readAccess = factory.createReadAccess(data, () -> 0);

        final LocalDateTime value = LocalDateTime.now();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());

        writeAccess.setObject(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getObject());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());
    }

    @Test
    public void testZonedDateTimeAccess() {
        final ZonedDateTimeAccessSpec spec = ZonedDateTimeAccessSpec.INSTANCE;
        @SuppressWarnings("unchecked")
        final ColumnarObjectAccessFactory<ZonedDateTime> factory =
            (ColumnarObjectAccessFactory<ZonedDateTime>)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.zonedDateTimeSpec(), factory.getColumnDataSpec());
        final TestZonedDateTimeData data = TestZonedDateTimeDataFactory.INSTANCE.createWriteData(1);

        final ObjectWriteAccess<ZonedDateTime> writeAccess = factory.createWriteAccess(data, () -> 0);
        final ObjectReadAccess<ZonedDateTime> readAccess = factory.createReadAccess(data, () -> 0);

        final ZonedDateTime value = ZonedDateTime.now();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());

        writeAccess.setObject(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getObject());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());
    }

    @Test
    public void testDurationAccess() {
        final DurationAccessSpec spec = DurationAccessSpec.INSTANCE;
        @SuppressWarnings("unchecked")
        final ColumnarObjectAccessFactory<Duration> factory =
            (ColumnarObjectAccessFactory<Duration>)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.durationSpec(), factory.getColumnDataSpec());
        final TestDurationData data = TestDurationDataFactory.INSTANCE.createWriteData(1);

        final ObjectWriteAccess<Duration> writeAccess = factory.createWriteAccess(data, () -> 0);
        final ObjectReadAccess<Duration> readAccess = factory.createReadAccess(data, () -> 0);

        final Duration value = Duration.ofHours(1);
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());

        writeAccess.setObject(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getObject());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());
    }

    @Test
    public void testPeriodAccess() {
        final PeriodAccessSpec spec = PeriodAccessSpec.INSTANCE;
        @SuppressWarnings("unchecked")
        final ColumnarObjectAccessFactory<Period> factory =
            (ColumnarObjectAccessFactory<Period>)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.periodSpec(), factory.getColumnDataSpec());
        final TestPeriodData data = TestPeriodDataFactory.INSTANCE.createWriteData(1);

        final ObjectWriteAccess<Period> writeAccess = factory.createWriteAccess(data, () -> 0);
        final ObjectReadAccess<Period> readAccess = factory.createReadAccess(data, () -> 0);

        final Period value = Period.ofWeeks(1);
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());

        writeAccess.setObject(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getObject());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getObject());
    }

}
