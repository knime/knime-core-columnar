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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListReadData;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListWriteData;
import org.knime.core.columnar.arrow.data.OnHeapVarBinaryData.OnHeapVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapVarBinaryData.OnHeapVarBinaryReadData;
import org.knime.core.columnar.arrow.data.OnHeapVarBinaryData.OnHeapVarBinaryWriteData;

/**
 * Test {@link OnHeapListData} with a list consisting of integer values.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class OnHeapVarBinaryListDataTest extends AbstractArrowDataTest<OnHeapListWriteData, OnHeapListReadData> {

    private static final int MAX_LENGTH = 4;

    private static final int MAX_OBJECT_SIZE = 3;

    /** Create the test for {@link OnHeapListData} */
    public OnHeapVarBinaryListDataTest() {
        super(new OnHeapListDataFactory(OnHeapVarBinaryDataFactory.INSTANCE));
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
        final int size = random.nextInt(MAX_LENGTH);
        final OnHeapVarBinaryWriteData inner = data.createWriteData(index, size);
        for (int i = 0; i < size; i++) {
            byte[] valueFor = valueFor(random);
            inner.setBytes(i, valueFor);
        }
    }

    @Override
    protected void checkValue(final OnHeapListReadData data, final int index, final int seed) {
        final OnHeapVarBinaryReadData element = data.createReadData(index);
        if (seed == 1) {
            assertEquals(0, element.length());
            return;
        }

        final Random random = new Random(seed);
        final int size = random.nextInt(MAX_LENGTH);
        assertEquals(size, element.length());

        for (int i = 0; i < size; i++) {
            byte[] object = element.getBytes(i);
            assertArrayEquals(valueFor(random), object);
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
    public void testSizeOf() { // NOSONAR
        // Do not test here
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        // Not tested here
        return 0;
    }

    private static byte[] valueFor(final Random random) {
        final byte[] bytes = new byte[random.nextInt(MAX_OBJECT_SIZE)];
        random.nextBytes(bytes);
        return bytes;
    }
}