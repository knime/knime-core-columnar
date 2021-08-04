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
 *   Jul 21, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.data.columnar.table.virtual.WriteAccessRowWriteTest.createRow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataRow;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Contains unit tests for {@link RowIteratorCursor}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@RunWith(MockitoJUnitRunner.class)
public class RowIteratorCursorTest {

    private RowIteratorCursor m_testInstance;

    @Mock
    private CloseableRowIterator m_iterator;

    @Before
    public void init() {
        m_testInstance = new RowIteratorCursor(TestHelper.SCHEMA, m_iterator);
    }

    @Test
    public void testClose() throws IOException {
        m_testInstance.close();
        verify(m_iterator).close();
    }

    @Test
    public void testIterating() {
        final DataRow first = createRow("first", false, 1, 1.4);
        final DataRow second = createRow("second", true, 3, 1.3);
        final DataRow third = createRow("third", false, 42, 13.37);
        when(m_iterator.hasNext()).thenReturn(true, true, true, false);
        when(m_iterator.next()).thenReturn(first, second, third, null);
        final ReadAccessRow access = m_testInstance.access();
        assertTrue(m_testInstance.forward());
        verifyValues(access, first);
        assertTrue(m_testInstance.forward());
        verifyValues(access, second);
        assertTrue(m_testInstance.forward());
        verifyValues(access, third);
        assertFalse(m_testInstance.forward());
    }

    static void verifyValues(final ReadAccessRow access, final DataRow row) {
        final StringReadAccess rowKeyAccess = access.getAccess(0);
        assertFalse(rowKeyAccess.isMissing());
        assertEquals(row.getKey().getString(), rowKeyAccess.getStringValue());
        final BooleanReadAccess booleanAccess = access.getAccess(1);
        final boolean booleanIsMissing = row.getCell(0).isMissing();
        assertEquals(booleanIsMissing, booleanAccess.isMissing());
        if (!booleanIsMissing) {
            assertEquals(((BooleanValue)row.getCell(0)).getBooleanValue(), booleanAccess.getBooleanValue());
        }
        final IntReadAccess intAccess = access.getAccess(2);
        final boolean intIsMissing = row.getCell(1).isMissing();
        assertEquals(intIsMissing, intAccess.isMissing());
        if (!intIsMissing) {
            assertEquals(((IntValue)row.getCell(1)).getIntValue(), intAccess.getIntValue());
        }
        final DoubleReadAccess doubleAccess = access.getAccess(3);
        final boolean doubleIsMissing = row.getCell(2).isMissing();
        assertEquals(doubleIsMissing, doubleAccess.isMissing());
        if (!doubleIsMissing) {
            assertEquals(((DoubleValue)row.getCell(2)).getDoubleValue(), doubleAccess.getDoubleValue(), 1e-8);
        }
    }
}
