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
 *   8 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.testing.data.TestLongData;
import org.knime.core.columnar.testing.data.TestLongData.TestLongDataFactory;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.LongCell.LongCellFactory;
import org.knime.core.data.v2.access.LongAccess.LongAccessSpec;
import org.knime.core.data.v2.access.LongAccess.LongReadAccess;
import org.knime.core.data.v2.access.LongAccess.LongWriteAccess;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@RunWith(Parameterized.class)
@SuppressWarnings("javadoc")
public class ColumnarLongAccessFactoryTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{Long.MIN_VALUE}, {0L}, {1L}, {Long.MAX_VALUE}}); // NOSONAR
    }

    private long m_value;

    public ColumnarLongAccessFactoryTest(final long value) {
        m_value = value;
    }

    @Test
    public void testAccesses() {

        final LongAccessSpec spec = LongAccessSpec.INSTANCE;
        final ColumnarLongAccessFactory factory = (ColumnarLongAccessFactory)ColumnarAccessFactoryMapper.INSTANCE.visit(spec);
        assertEquals(DataSpec.longSpec(), factory.getColumnDataSpec());
        final TestLongData data = TestLongDataFactory.INSTANCE.createWriteData(1);
        final LongWriteAccess writeAccess = factory.createWriteAccess(data, () -> 0);
        final LongReadAccess readAccess = factory.createReadAccess(data, () -> 0);

        final LongCell cell = (LongCell)LongCellFactory.create(m_value);

        // set cell
        assertTrue(readAccess.isMissing());
        try {
            readAccess.getLongValue();
            fail();
        } catch (NullPointerException e) {
        }
        writeAccess.setValue(cell);
        assertFalse(readAccess.isMissing());
        assertEquals(m_value, readAccess.getLongValue());

        // set missing
        writeAccess.setMissing();
        assertTrue(readAccess.isMissing());
        try {
            readAccess.getLongValue();
            fail();
        } catch (NullPointerException e) {
        }

        // set value
        writeAccess.setLongValue(m_value);
        assertFalse(readAccess.isMissing());
        assertEquals(cell, readAccess.getDataCell());
        assertEquals(cell.getDoubleValue(), readAccess.getDoubleValue(), 0d);
        assertEquals(cell.getRealValue(), readAccess.getRealValue(), 0d);
        assertEquals(cell.getImaginaryValue(), readAccess.getImaginaryValue(), 0d);
        assertEquals(cell.getMinSupport(), readAccess.getMinSupport(), 0d);
        assertEquals(cell.getCore(), readAccess.getCore(), 0d);
        assertEquals(cell.getMaxSupport(), readAccess.getMaxSupport(), 0d);
        assertEquals(cell.getMinCore(), readAccess.getMinCore(), 0d);
        assertEquals(cell.getMaxCore(), readAccess.getMaxCore(), 0d);
        assertEquals(cell.getCenterOfGravity(), readAccess.getCenterOfGravity(), 0d);
    }

}
