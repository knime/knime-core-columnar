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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.OnHeapLongData.OnHeapLongDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapLongData.OnHeapLongReadData;
import org.knime.core.columnar.arrow.data.OnHeapLongData.OnHeapLongWriteData;

/**
 * Test {@link OnHeapLongData}
 *
 * Tests for reading and writing long values using the OnHeapLongData structures.
 */
public class OnHeapLongDataTest extends AbstractArrowDataTest<OnHeapLongWriteData, OnHeapLongReadData> {

    /** Create the test for {@link OnHeapLongData} */
    public OnHeapLongDataTest() {
        super(OnHeapLongDataFactory.INSTANCE);
    }

    @Override
    protected OnHeapLongWriteData castW(final Object o) {
        assertTrue(o instanceof OnHeapLongWriteData, "Object is not an instance of OnHeapLongWriteData");
        return (OnHeapLongWriteData)o;
    }

    @Override
    protected OnHeapLongReadData castR(final Object o) {
        assertTrue(o instanceof OnHeapLongReadData, "Object is not an instance of OnHeapLongReadData");
        return (OnHeapLongReadData)o;
    }

    private static long valueFor(final int seed) {
        // For demonstration, just return seed * 10 as a long value.
        return seed * 10L;
    }

    @Override
    protected void setValue(final OnHeapLongWriteData data, final int index, final int seed) {
        data.setLong(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final OnHeapLongReadData data, final int index, final int seed) {
        assertEquals(valueFor(seed), data.getLong(index),
            "Value at index " + index + " does not match expected value for seed " + seed);
    }

    @Override
    protected boolean isReleasedW(final OnHeapLongWriteData data) {
        // On-heap data does not allocate off-heap resources that need explicit releasing
        return false;
    }

    @Override
    protected boolean isReleasedR(final OnHeapLongReadData data) {
        // On-heap data does not allocate off-heap resources that need explicit releasing
        return false;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        return 8L * capacity // 8 bytes per value for a long
            + (long)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer
    }
}
