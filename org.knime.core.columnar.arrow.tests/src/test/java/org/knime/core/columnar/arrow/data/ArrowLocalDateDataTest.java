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

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.Random;

import org.apache.arrow.vector.BigIntVector;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowLocalDateData.ArrowLocalDateDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLocalDateData.ArrowLocalDateReadData;
import org.knime.core.columnar.arrow.data.ArrowLocalDateData.ArrowLocalDateWriteData;

/**
 * Test {@link ArrowLocalDateData}
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowLocalDateDataTest extends AbstractArrowDataTest<ArrowLocalDateWriteData, ArrowLocalDateReadData> {

    static final long MIN_EPOCH_DAY = LocalDate.MIN.getLong(ChronoField.EPOCH_DAY);

    static final long MAX_EPOCH_DAY = LocalDate.MAX.getLong(ChronoField.EPOCH_DAY);

    /** Create the test for {@link ArrowLocalDateData} */
    public ArrowLocalDateDataTest() {
        super(ArrowLocalDateDataFactory.INSTANCE);
    }

    @Override
    protected ArrowLocalDateWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowLocalDateWriteData);
        return (ArrowLocalDateWriteData)o;
    }

    @Override
    protected ArrowLocalDateReadData castR(final Object o) {
        assertTrue(o instanceof ArrowLocalDateReadData);
        return (ArrowLocalDateReadData)o;
    }

    @Override
    protected void setValue(final ArrowLocalDateWriteData data, final int index, final int seed) {
        data.setLocalDate(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowLocalDateReadData data, final int index, final int seed) {
        assertEquals(valueFor(seed), data.getLocalDate(index));
    }

    @Override
    protected boolean isReleasedW(final ArrowLocalDateWriteData data) {
        return data.m_vector == null;
    }

    @Override
    @SuppressWarnings("resource") // Resources handled by vector
    protected boolean isReleasedR(final ArrowLocalDateReadData data) {
        final ArrowLocalDateReadData cast = castR(data);
        final BigIntVector v = cast.m_vector;
        return v.getDataBuffer().capacity() == 0 && v.getValidityBuffer().capacity() == 0;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        return capacity * 8 // 8 byte per value for data
            + (long)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer
    }

    private static LocalDate valueFor(final int seed) {
        final Random random = new Random(seed);
        final long epochDay = MIN_EPOCH_DAY + (long)(random.nextDouble() * (MAX_EPOCH_DAY - MIN_EPOCH_DAY));
        return LocalDate.ofEpochDay(epochDay);
    }
}
