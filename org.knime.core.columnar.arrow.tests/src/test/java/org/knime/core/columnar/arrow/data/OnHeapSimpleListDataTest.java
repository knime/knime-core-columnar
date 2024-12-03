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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntReadData;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntWriteData;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListReadData;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListWriteData;

/**
 * Test {@link OnHeapListData} with a list consisting of integer values.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class OnHeapSimpleListDataTest extends AbstractArrowDataTest<OnHeapListWriteData, OnHeapListReadData> {

    private static final int MAX_LENGTH = 100;

    /** Create the test for {@link OnHeapListData} */
    public OnHeapSimpleListDataTest() {
        super(new OnHeapListData.Factory(OnHeapIntData.FACTORY));
    }

    @Override
    protected OnHeapListWriteData castW(final Object o) {
        assertTrue(o instanceof OnHeapListWriteData, "Object is not an instance of OnHeapListWriteData");
        return (OnHeapListWriteData)o;
    }

    @Override
    protected OnHeapListReadData castR(final Object o) {
        assertTrue(o instanceof OnHeapListReadData, "Object is not an instance of OnHeapListReadData");
        return (OnHeapListReadData)o;
    }

    @Override
    protected void setValue(final OnHeapListWriteData data, final int index, final int seed) {
        if (seed == 1) {
            // Test the special case with an empty list
            data.createWriteData(index, 0);
            return;
        }

        final Random random = new Random(seed);
        final int size = random.nextInt(MAX_LENGTH);
        final OnHeapIntWriteData inner = data.createWriteData(index, size);
        for (int i = 0; i < size; i++) {
            inner.setInt(i, random.nextInt());
        }
    }

    @Override
    protected void checkValue(final OnHeapListReadData data, final int index, final int seed) {
        final OnHeapIntReadData element = data.createReadData(index);
        if (seed == 1) {
            assertEquals(0, element.length(), "List length should be 0 for seed 1");
            return;
        }

        final Random random = new Random(seed);
        final int size = random.nextInt(MAX_LENGTH);
        assertEquals(size, element.length(), "List length does not match expected size for seed " + seed);

        for (int i = 0; i < size; i++) {
            assertEquals(random.nextInt(), element.getInt(i),
                "List element at index " + i + " does not match expected value for seed " + seed);
        }
    }

    @Override
    protected boolean isReleasedW(final OnHeapListWriteData data) {
        // On-heap data does not have resources that need explicit release
        return false;
    }

    @Override
    protected boolean isReleasedR(final OnHeapListReadData data) {
        // On-heap data does not have resources that need explicit release
        return false;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        final long innerValueCount = getInnerValueCount(valueCount);
        return capacity * Integer.BYTES // size of m_offsets
            + innerValueCount * Integer.BYTES // size of m_data
            + (long)Math.ceil(capacity / 8.0) // Validity buffer
            + (long)Math.ceil(innerValueCount / 8.0); // Inner: validity buffer
    }

    private static long getInnerValueCount(final int valueCount) {
        long c = 0;
        for (int i = 0; i < valueCount; i++) {
            c += i == 1 ? 0 : new Random(i).nextInt(MAX_LENGTH);
        }
        return c;
    }
}
