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

import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntReadData;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntWriteData;

/**
 * Test {@link OnHeapIntData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class OnHeapIntDataTest extends AbstractArrowDataTest<OnHeapIntWriteData, OnHeapIntReadData> {

    /** Create the test for {@link OnHeapIntData} */
    public OnHeapIntDataTest() {
        super(OnHeapIntDataFactory.INSTANCE);
    }

    @Override
    protected OnHeapIntWriteData castW(final Object o) {
        assertTrue(o instanceof OnHeapIntWriteData, "Object is not an instance of OnHeapIntWriteData");
        return (OnHeapIntWriteData)o;
    }

    @Override
    protected OnHeapIntReadData castR(final Object o) {
        assertTrue(o instanceof OnHeapIntReadData, "Object is not an instance of OnHeapIntReadData");
        return (OnHeapIntReadData)o;
    }

    private static int valueFor(final int seed) {
        return seed * 2;
    }

    @Override
    protected void setValue(final OnHeapIntWriteData data, final int index, final int seed) {
        data.setInt(index, valueFor(seed));
    }

    @Override
    protected void checkValue(final OnHeapIntReadData data, final int index, final int seed) {
        assertEquals(valueFor(seed), data.getInt(index),
            "Value at index " + index + " does not match expected value for seed " + seed);
    }

    @Override
    protected boolean isReleasedW(final OnHeapIntWriteData data) {
        return false;
    }

    @Override
    protected boolean isReleasedR(final OnHeapIntReadData data) {
        return false;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        return 4L * capacity // 4 bytes per value for data
            + (long)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer
    }
}
