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
package org.knime.core.columnar.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.knime.core.columnar.table.ColumnarTableTestUtils.TestDoubleColumnType.TestDoubleReadValue;
import org.knime.core.columnar.table.ColumnarTableTestUtils.TestDoubleColumnType.TestDoubleWriteValue;

public class CursorTests {

    @Test
    public void testIdentity() throws Exception {
        int numRows = 107;
        int numColumns = 1;
        int chunkSize = 10;

        WriteTable wTable = ColumnarTableTestUtils.createWriteTable(numColumns, chunkSize);

        fillWriteTable(numRows, wTable);

        // create read table
        ReadTable rTable = wTable.createReadTable();

        assertEquals(numColumns, rTable.getNumColumns());
        assertEquals(numColumns, rTable.getSchema().getNumColumns());

        // test cursor
        try (TableReadCursor rCursor = rTable.cursor()) {
            readIdentity(0, numRows - 1, rCursor);
        }
    }

    @Test
    public void testTableFilter() throws Exception {
        int numRows = 107;
        int numColumns = 1;
        int chunkSize = 10;

        WriteTable wTable = ColumnarTableTestUtils.createWriteTable(numColumns, chunkSize);

        fillWriteTable(numRows, wTable);

        // create read table
        ReadTable rTable = wTable.createReadTable();
        // test cursor with filter which doesn't actually filter
        try (TableReadCursor rCursor =
            rTable.cursor(ColumnarTableTestUtils.createTableReadCursorConfig(0, numRows - 1, 0))) {
            readIdentity(0, numRows - 1, rCursor);
        }

        // test random subset
        try (TableReadCursor rCursor = rTable.cursor(ColumnarTableTestUtils.createTableReadCursorConfig(42, 99, 0))) {
            readIdentity(42, 99, rCursor);
        }

        // test cursor with filter min-max
        try (TableReadCursor rCursor =
            rTable.cursor(ColumnarTableTestUtils.createTableReadCursorConfig(42, numRows - 1, 0))) {
            readIdentity(42, numRows - 1, rCursor);
        }

        rTable.close();
    }

    private void fillWriteTable(final int numRows, final WriteTable wTable) throws Exception {
        try (TableWriteCursor wCursor = wTable.getCursor()) {
            TestDoubleWriteValue wValue = wCursor.get(0);

            for (int i = 0; i < numRows; i++) {
                wCursor.fwd();
                if (i % 13 == 0) {
                    wValue.setMissing();
                } else {
                    wValue.setDouble(i);
                }
            }
        }
    }

    private void readIdentity(final int firstIndex, final int lastIndex, final TableReadCursor rCursor) throws Exception {
        TestDoubleReadValue rValue = rCursor.get(0);

        int i = firstIndex - 1;
        while (rCursor.canFwd()) {
            rCursor.fwd();
            i++;
            if (i % 13 == 0) {
                assertTrue(rValue.isMissing());
            } else {
                assertFalse(rValue.isMissing());
                assertEquals(rValue.getDouble(), i, 0);
            }
        }
        assertEquals(lastIndex, i);
    }
}
