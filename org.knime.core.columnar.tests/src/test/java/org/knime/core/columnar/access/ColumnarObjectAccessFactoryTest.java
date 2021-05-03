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
package org.knime.core.columnar.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;

import org.junit.Test;
import org.knime.core.columnar.access.ColumnarDurationAccessFactory.ColumnarDurationReadAccess;
import org.knime.core.columnar.access.ColumnarDurationAccessFactory.ColumnarDurationWriteAccess;
import org.knime.core.columnar.access.ColumnarLocalDateAccessFactory.ColumnarLocalDateReadAccess;
import org.knime.core.columnar.access.ColumnarLocalDateAccessFactory.ColumnarLocalDateWriteAccess;
import org.knime.core.columnar.access.ColumnarLocalDateTimeAccessFactory.ColumnarLocalDateTimeReadAccess;
import org.knime.core.columnar.access.ColumnarLocalDateTimeAccessFactory.ColumnarLocalDateTimeWriteAccess;
import org.knime.core.columnar.access.ColumnarLocalTimeAccessFactory.ColumnarLocalTimeReadAccess;
import org.knime.core.columnar.access.ColumnarLocalTimeAccessFactory.ColumnarLocalTimeWriteAccess;
import org.knime.core.columnar.access.ColumnarPeriodAccessFactory.ColumnarPeriodReadAccess;
import org.knime.core.columnar.access.ColumnarPeriodAccessFactory.ColumnarPeriodWriteAccess;
import org.knime.core.columnar.access.ColumnarStringAccessFactory.ColumnarStringReadAccess;
import org.knime.core.columnar.access.ColumnarStringAccessFactory.ColumnarStringWriteAccess;
import org.knime.core.columnar.access.ColumnarZonedDateTimeAccessFactory.ColumnarZonedDateTimeReadAccess;
import org.knime.core.columnar.access.ColumnarZonedDateTimeAccessFactory.ColumnarZonedDateTimeWriteAccess;
import org.knime.core.columnar.testing.data.TestDurationData;
import org.knime.core.columnar.testing.data.TestDurationData.TestDurationDataFactory;
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
import org.knime.core.table.schema.DurationDataSpec;
import org.knime.core.table.schema.LocalDateDataSpec;
import org.knime.core.table.schema.LocalDateTimeDataSpec;
import org.knime.core.table.schema.PeriodDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.ZonedDateTimeDataSpec;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarObjectAccessFactoryTest {

    @Test
    public void testStringAccess() {
        final StringDataSpec spec = StringDataSpec.INSTANCE;
        final ColumnarStringAccessFactory factory =
            (ColumnarStringAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        final TestStringData data = TestStringDataFactory.INSTANCE.createWriteData(1);

        final ColumnarStringWriteAccess writeAccess = factory.createWriteAccess(() -> 0);
        writeAccess.setData(data);
        final ColumnarStringReadAccess readAccess = factory.createReadAccess(() -> 0);
        readAccess.setData(data);

        final String value = "test";
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getStringValue());

        writeAccess.setStringValue(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getStringValue());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getStringValue());
    }

    @Test
    public void testLocalDateAccess() {
        final LocalDateDataSpec spec = LocalDateDataSpec.INSTANCE;
        final ColumnarLocalDateAccessFactory factory =
            (ColumnarLocalDateAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        final TestLocalDateData data = TestLocalDateDataFactory.INSTANCE.createWriteData(1);

        final ColumnarLocalDateWriteAccess writeAccess = factory.createWriteAccess(() -> 0);
        writeAccess.setData(data);
        final ColumnarLocalDateReadAccess readAccess = factory.createReadAccess(() -> 0);
        readAccess.setData(data);

        final LocalDate value = LocalDate.now();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getLocalDateValue());

        writeAccess.setLocalDateValue(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getLocalDateValue());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getLocalDateValue());
    }

    @Test
    public void testLocalTimeAccess() {
        final ColumnarLocalTimeAccessFactory factory = ColumnarLocalTimeAccessFactory.INSTANCE;
        final TestLocalTimeData data = TestLocalTimeDataFactory.INSTANCE.createWriteData(1);

        final ColumnarLocalTimeWriteAccess writeAccess = factory.createWriteAccess(() -> 0);
        writeAccess.setData(data);
        final ColumnarLocalTimeReadAccess readAccess = factory.createReadAccess(() -> 0);
        readAccess.setData(data);

        final LocalTime value = LocalTime.now();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getLocalTimeValue());

        writeAccess.setLocalTimeValue(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getLocalTimeValue());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getLocalTimeValue());
    }

    @Test
    public void testLocalDateTimeAccess() {
        final LocalDateTimeDataSpec spec = LocalDateTimeDataSpec.INSTANCE;
        final ColumnarLocalDateTimeAccessFactory factory =
            (ColumnarLocalDateTimeAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        final TestLocalDateTimeData data = TestLocalDateTimeDataFactory.INSTANCE.createWriteData(1);

        final ColumnarLocalDateTimeWriteAccess writeAccess = factory.createWriteAccess(() -> 0);
        writeAccess.setData(data);
        final ColumnarLocalDateTimeReadAccess readAccess = factory.createReadAccess(() -> 0);
        readAccess.setData(data);

        final LocalDateTime value = LocalDateTime.now();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getLocalDateTimeValue());

        writeAccess.setLocalDateTimeValue(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getLocalDateTimeValue());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getLocalDateTimeValue());
    }

    @Test
    public void testZonedDateTimeAccess() {
        final ZonedDateTimeDataSpec spec = ZonedDateTimeDataSpec.INSTANCE;
        final ColumnarZonedDateTimeAccessFactory factory =
            (ColumnarZonedDateTimeAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        final TestZonedDateTimeData data = TestZonedDateTimeDataFactory.INSTANCE.createWriteData(1);

        final ColumnarZonedDateTimeWriteAccess writeAccess = factory.createWriteAccess(() -> 0);
        writeAccess.setData(data);
        final ColumnarZonedDateTimeReadAccess readAccess = factory.createReadAccess(() -> 0);
        readAccess.setData(data);

        final ZonedDateTime value = ZonedDateTime.now();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getZonedDateTimeValue());

        writeAccess.setZonedDateTimeValue(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getZonedDateTimeValue());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getZonedDateTimeValue());
    }

    @Test
    public void testDurationAccess() {
        final DurationDataSpec spec = DurationDataSpec.INSTANCE;
        final ColumnarDurationAccessFactory factory =
            (ColumnarDurationAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        final TestDurationData data = TestDurationDataFactory.INSTANCE.createWriteData(1);

        final ColumnarDurationWriteAccess writeAccess = factory.createWriteAccess(() -> 0);
        writeAccess.setData(data);
        final ColumnarDurationReadAccess readAccess = factory.createReadAccess(() -> 0);
        readAccess.setData(data);

        final Duration value = Duration.ofHours(1);
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getDurationValue());

        writeAccess.setDurationValue(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getDurationValue());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getDurationValue());
    }

    @Test
    public void testPeriodAccess() {
        final PeriodDataSpec spec = PeriodDataSpec.INSTANCE;
        final ColumnarPeriodAccessFactory factory =
            (ColumnarPeriodAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        final TestPeriodData data = TestPeriodDataFactory.INSTANCE.createWriteData(1);

        final ColumnarPeriodWriteAccess writeAccess = factory.createWriteAccess(() -> 0);
        writeAccess.setData(data);
        final ColumnarPeriodReadAccess readAccess = factory.createReadAccess(() -> 0);
        readAccess.setData(data);

        final Period value = Period.ofWeeks(1);
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getPeriodValue());

        writeAccess.setPeriodValue(value);
        assertFalse(readAccess.isMissing());
        assertEquals(value, readAccess.getPeriodValue());

        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        assertNull(readAccess.getPeriodValue());
    }

}
