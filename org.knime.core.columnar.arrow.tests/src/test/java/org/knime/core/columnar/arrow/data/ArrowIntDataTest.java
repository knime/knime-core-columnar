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

import org.knime.core.columnar.arrow.AbstractArrowDataTest;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntReadData;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntWriteData;

/**
 * Test {@link ArrowIntData}
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowIntDataTest extends AbstractArrowDataTest<ArrowIntWriteData, ArrowIntReadData> {

    /** Create the test for {@link ArrowIntData} */
    public ArrowIntDataTest() {
        super(ArrowIntDataFactory.INSTANCE);
    }

    @Override
    protected ArrowIntWriteData castW(final Object o) {
        assertTrue(o instanceof ArrowIntWriteData);
        return (ArrowIntWriteData)o;
    }

    @Override
    protected ArrowIntReadData castR(final Object o) {
        assertTrue(o instanceof ArrowIntReadData);
        return (ArrowIntReadData)o;
    }

    @Override
    protected void setValue(final ArrowIntWriteData data, final int index, final int seed) {
        data.setInt(index, seed);
    }

    @Override
    protected void checkValue(final ArrowIntReadData data, final int index, final int seed) {
        assertEquals(seed, data.getInt(index));
    }

    @Override
    protected boolean isReleasedW(final ArrowIntWriteData data) {
        return data.m_vector == null;
    }

    @Override
    @SuppressWarnings("resource")
    protected boolean isReleasedR(final ArrowIntReadData data) {
        return data.m_vector.getDataBuffer().capacity() == 0 && data.m_vector.getValidityBuffer().capacity() == 0;
    }

    @Override
    protected long getMinSize(final int valueCount, final int capacity) {
        return 4 * capacity // 4 bytes per value for data
            + (long)Math.ceil(capacity / 8.0); // 1 bit per value for validity buffer
    }
}
