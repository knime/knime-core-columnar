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

import java.time.Period;
import java.util.Random;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowPeriodData.ArrowPeriodDataFactory;
import org.knime.core.columnar.arrow.data.ArrowPeriodData.ArrowPeriodReadData;
import org.knime.core.columnar.arrow.data.ArrowPeriodData.ArrowPeriodWriteData;

/**
 * Test {@link ArrowPeriodData}
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowPeriodDataTest extends AbstractArrowDataTest<ArrowPeriodWriteData, ArrowPeriodReadData> {

    /** Create the test for {@link ArrowPeriodData} */
    public ArrowPeriodDataTest() {
        super(ArrowPeriodDataFactory.INSTANCE);
    }

    @Override
    protected ArrowPeriodWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowPeriodWriteData);
        return (ArrowPeriodWriteData)o;
    }

    @Override
    protected ArrowPeriodReadData castR(final Object o) {
        assertTrue(o instanceof ArrowPeriodReadData);
        return (ArrowPeriodReadData)o;
    }

    @Override
    protected void setValue(final ArrowPeriodWriteData data, final int index, final int seed) {
        data.setPeriod(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final ArrowPeriodReadData data, final int index, final int seed) {
        assertEquals(valueFor(seed), data.getPeriod(index));
    }

    @Override
    protected boolean isReleasedW(final ArrowPeriodWriteData data) {
        return data.m_vector == null;
    }

    @Override
    @SuppressWarnings("resource") // Resources handled by vector
    protected boolean isReleasedR(final ArrowPeriodReadData data) {
        final ArrowPeriodReadData cast = castR(data);
        final StructVector v = cast.m_vector;
        final IntVector y = cast.m_yearsVector;
        final IntVector m = cast.m_monthsVector;
        final IntVector d = cast.m_daysVector;
        final boolean yReleased = y.getDataBuffer().capacity() == 0 && y.getValidityBuffer().capacity() == 0;
        final boolean mReleased = m.getDataBuffer().capacity() == 0 && m.getValidityBuffer().capacity() == 0;
        final boolean dReleased = d.getDataBuffer().capacity() == 0 && d.getValidityBuffer().capacity() == 0;
        return v.getValidityBuffer().capacity() == 0 && yReleased && mReleased && dReleased;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        return capacity * 3 * 4 // 3 * 4 byte per value for data
            + 4 * (long)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer (for struct and all children)
    }

    private static Period valueFor(final int seed) {
        final Random random = new Random(seed);
        final int years = random.nextInt();
        final int months = random.nextInt();
        final int days = random.nextInt();
        return Period.of(years, months, days);
    }
}
