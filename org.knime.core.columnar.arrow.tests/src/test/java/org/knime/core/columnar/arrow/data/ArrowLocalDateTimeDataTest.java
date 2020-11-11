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
 *
 * History
 *   Sep 30, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.arrow.data.ArrowLocalDateDataTest.MAX_EPOCH_DAY;
import static org.knime.core.columnar.arrow.data.ArrowLocalDateDataTest.MIN_EPOCH_DAY;
import static org.knime.core.columnar.arrow.data.ArrowLocalTimeDataTest.NANOS_PER_DAY;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Random;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.complex.StructVector;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowLocalDateTimeData.ArrowLocalDateTimeDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLocalDateTimeData.ArrowLocalDateTimeReadData;
import org.knime.core.columnar.arrow.data.ArrowLocalDateTimeData.ArrowLocalDateTimeWriteData;

/**
 * Test {@link ArrowLocalDateTimeData}
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowLocalDateTimeDataTest
    extends AbstractArrowDataTest<ArrowLocalDateTimeWriteData, ArrowLocalDateTimeReadData> {

    /** Create the test for {@link ArrowLocalDateTimeData} */
    public ArrowLocalDateTimeDataTest() {
        super(ArrowLocalDateTimeDataFactory.INSTANCE);
    }

    @Override
    protected ArrowLocalDateTimeWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowLocalDateTimeWriteData);
        return (ArrowLocalDateTimeWriteData)o;
    }

    @Override
    protected ArrowLocalDateTimeReadData castR(final Object o) {
        assertTrue(o instanceof ArrowLocalDateTimeReadData);
        return (ArrowLocalDateTimeReadData)o;
    }

    @Override
    protected void setValue(final ArrowLocalDateTimeWriteData data, final int index, final int seed) {
        data.setLocalDateTime(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowLocalDateTimeReadData data, final int index, final int seed) {
        assertEquals(valueFor(seed), data.getLocalDateTime(index));
    }

    @Override
    protected boolean isReleasedW(final ArrowLocalDateTimeWriteData data) {
        return data.m_vector == null;
    }

    @Override
    @SuppressWarnings("resource") // Resources handled by vector
    protected boolean isReleasedR(final ArrowLocalDateTimeReadData data) {
        final ArrowLocalDateTimeReadData cast = castR(data);
        final StructVector v = cast.m_vector;
        final BigIntVector d = cast.m_epochDayVector;
        final TimeNanoVector t = cast.m_nanoOfDayVector;
        final boolean dReleased = d.getDataBuffer().capacity() == 0 && d.getValidityBuffer().capacity() == 0;
        final boolean tReleased = t.getDataBuffer().capacity() == 0 && t.getValidityBuffer().capacity() == 0;
        return v.getValidityBuffer().capacity() == 0 && dReleased && tReleased;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        return capacity * 3 * 4 // 3 * 4 byte per value for data
            + 4 * (long)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer (for struct and all children)
    }

    private static LocalDateTime valueFor(final int seed) {
        final Random random = new Random(seed);
        final long epochDay = MIN_EPOCH_DAY + (long)(random.nextDouble() * (MAX_EPOCH_DAY - MIN_EPOCH_DAY));
        final long nanoOfDay = (long)(random.nextDouble() * NANOS_PER_DAY);
        return LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay));
    }
}
