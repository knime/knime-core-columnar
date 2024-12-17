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
 *   Oct 22, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntReadData;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntWriteData;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListReadData;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListWriteData;

/**
 * Test {@link OnHeapListData} with a recursive lists.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class OnHeapRecursiveListDataTest extends AbstractArrowDataTest<OnHeapListWriteData, OnHeapListReadData> {

    private static final int MAX_LENGTH_1 = 5;

    private static final int MAX_LENGTH_2 = 5;

    /** Create the test for {@link OnHeapListData} */
    public OnHeapRecursiveListDataTest() {
        super(createFactory());
    }

    private static OnHeapListDataFactory createFactory() {
        return new OnHeapListDataFactory(new OnHeapListDataFactory(OnHeapIntDataFactory.INSTANCE));
    }

    @Override
    protected OnHeapListWriteData castW(final Object o) {
        assertTrue(o instanceof OnHeapListWriteData);
        return (OnHeapListWriteData)o;
    }

    @Override
    protected OnHeapListReadData castR(final Object o) {
        assertTrue(o instanceof OnHeapListReadData);
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
        final int size1 = random.nextInt(MAX_LENGTH_1);
        final int[] size2 = getInnerSizes(random, size1);
        final OnHeapListWriteData inner1 = data.createWriteData(index, size1);
        for (int i = 0; i < size1; i++) {
            final OnHeapIntWriteData inner2 = inner1.createWriteData(i, size2[i]);
            for (int j = 0; j < size2[i]; j++) {
                inner2.setInt(j, random.nextInt());
            }
        }
    }

    @Override
    protected void checkValue(final OnHeapListReadData data, final int index, final int seed) {
        final OnHeapListReadData inner1 = data.createReadData(index);
        if (seed == 1) {
            assertEquals(0, inner1.length());
            return;
        }

        final Random random = new Random(seed);
        final int size1 = random.nextInt(MAX_LENGTH_1);
        final int[] size2 = getInnerSizes(random, size1);
        assertEquals(size1, inner1.length());

        for (int i = 0; i < size1; i++) {
            final OnHeapIntReadData inner2 = inner1.createReadData(i);
            assertEquals(size2[i], inner2.length());
            for (int j = 0; j < size2[i]; j++) {
                assertEquals(random.nextInt(), inner2.getInt(j));
            }
        }
    }

    @Override
    protected boolean isReleasedW(final OnHeapListWriteData data) {
        return false;
    }

    @Override
    protected boolean isReleasedR(final OnHeapListReadData data) {
        return false;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        return (long)Math.ceil(capacity / 8.0) // Validity buffer
            + (capacity + 1) * 4 // Offset buffer
            + getInnerMinSize(valueCount);
    }

    private static long getInnerMinSize(final int valueCount) {
        long inner1Count = 0;
        long inner2Count = 0;
        for (int i = 0; i < valueCount; i++) {
            if (i != 1) {
                final Random random = new Random(i);
                final int listSize = random.nextInt(MAX_LENGTH_1);
                inner1Count += listSize;
                for (int list2Size : getInnerSizes(random, listSize)) {
                    inner2Count += list2Size;
                }
            }
        }
        return (long)Math.ceil(inner1Count / 8.0) // Inner1: validity buffer
            + (long)Math.ceil(inner2Count / 8.0) // Inner2: validity buffer
            + (inner1Count + 1) * 4 // Inner1: Offset buffer
            + inner2Count * 4; // Inner2: data buffer
    }

    private static int[] getInnerSizes(final Random random, final int size) {
        final int[] innerSizes = new int[size];
        for (int i = 0; i < size; i++) {
            innerSizes[i] = random.nextInt(MAX_LENGTH_2);
        }
        return innerSizes;
    }
}
