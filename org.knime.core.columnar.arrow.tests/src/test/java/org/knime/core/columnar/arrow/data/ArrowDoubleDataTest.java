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

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowDoubleData.ArrowDoubleDataFactory;

/**
 * Test {@link ArrowDoubleData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowDoubleDataTest extends AbstractArrowDataTest<ArrowDoubleData, ArrowDoubleData> {

    /** Create the test for {@link ArrowDoubleData} */
    public ArrowDoubleDataTest() {
        super(ArrowDoubleDataFactory.INSTANCE);
    }

    private static ArrowDoubleData cast(final Object o) {
        assertTrue(o instanceof ArrowDoubleData);
        return (ArrowDoubleData)o;
    }

    @Override
    protected ArrowDoubleData castW(final Object o) {
        return cast(o);
    }

    @Override
    protected ArrowDoubleData castR(final Object o) {
        return cast(o);
    }

    @Override
    protected void setValue(final ArrowDoubleData data, final int index, final int seed) {
        data.setDouble(index, seed);
    }

    @Override
    protected void checkValue(final ArrowDoubleData data, final int index, final int seed) {
        assertEquals(seed, data.getDouble(index), 0);
    }

    @Override
    @SuppressWarnings("resource") // Resources handled by vector
    protected boolean isReleased(final ReferencedData data) {
        final Float8Vector v = cast(data).m_vector;
        return v.getDataBuffer().capacity() == 0 && v.getValidityBuffer().capacity() == 0;
    }

    @Override
    protected int getMinSize(final int valueCount, final int capacity) {
        return 8 * capacity // 8 bytes per value for data
            + (int)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer
    }

}
