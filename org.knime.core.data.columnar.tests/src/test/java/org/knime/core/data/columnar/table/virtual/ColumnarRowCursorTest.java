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
 *   Jul 26, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.knime.core.data.columnar.table.ColumnarRowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Contains unit tests for {@link ColumnarRowCursor}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@RunWith(MockitoJUnitRunner.class)
public class ColumnarRowCursorTest {

    @Mock
    private LookaheadCursor<ReadAccessRow> m_cursor;

    @Mock
    private RowRead m_rowRead;

    private ColumnarRowCursor m_testInstance;

    @Before
    public void init() {
        m_testInstance = new ColumnarRowCursor(m_rowRead, m_cursor);
    }

    @Test
    public void testCanForward() {
        when(m_cursor.canForward()).thenReturn(true, true, false);
        when(m_cursor.forward()).thenReturn(true, true, false);
        assertTrue(m_testInstance.canForward());
        assertEquals(m_rowRead, m_testInstance.forward());
        assertTrue(m_testInstance.canForward());
        assertEquals(m_rowRead, m_testInstance.forward());
        assertFalse(m_testInstance.canForward());
        assertNull(m_testInstance.forward());
    }

    @Test
    public void testGetNumColumns() {
        when(m_rowRead.getNumColumns()).thenReturn(4);
        assertEquals(4, m_testInstance.getNumColumns());
    }

    @Test
    public void testCloseWithouException() throws IOException {
        m_cursor.close();
        verify(m_cursor).close();
    }

    @Test
    public void testCloseWithException() throws IOException {
        doThrow(new IOException("Failing to close")).when(m_cursor).close();
        // the cursor is supposed to swallow (and log) IOExceptions of the underlying cursor
        m_testInstance.close();
        verify(m_cursor).close();
    }
}
