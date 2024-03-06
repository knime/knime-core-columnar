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
 *   Feb 29, 2024 (benjamin): created
 */
package org.knime.core.columnar.cursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.util.stream.IntStream;

import org.junit.Test;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.DefaultTestBatchStore;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.columnar.testing.data.TestIntData;
import org.knime.core.columnar.testing.data.TestStringData;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Tests for the {@link DefaultColumnarCursor}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class DefaultColumnarCursorTest extends ColumnarTest {

    @Test
    public void testSameAccessInstance() throws IOException {
        var batches = new int[]{2, 2, 1};

        try ( //
                var store = createData(batches); //
                var cursor = new DefaultColumnarCursor(store, Selection.all()) //
        ) {
            var access = cursor.access();
            var intAccess = access.getAccess(0);
            var stringAccess = access.getAccess(1);
            while (cursor.forward()) {
                assertSame("must always be the same access instance", access, cursor.access());
                assertSame("must always be the same access instance", intAccess, access.getAccess(0));
                assertSame("must always be the same access instance", stringAccess, access.getAccess(1));
            }
        }
    }

    @Test
    public void testIterationAll() throws IOException {
        var batches = new int[]{2, 2, 1};
        var numRows = numRows(batches);

        try ( //
                var store = createData(batches); //
                var cursor = new DefaultColumnarCursor(store, Selection.all()) //
        ) {
            var access = cursor.access();
            assertEquals("size of the ReadAccessRow should be number of columns", 2, access.size());
            var intAccess = access.getAccess(0);
            var stringAccess = access.getAccess(1);
            var rowIdx = 0;
            assertTrue("should canForward", cursor.canForward());
            while (cursor.forward()) {
                checkIntValue(rowIdx, intAccess);
                checkStringValue(rowIdx, stringAccess);
                rowIdx++;
                assertEquals("should canForward", rowIdx != numRows, cursor.canForward());
            }
            assertEquals("should read num rows", numRows, rowIdx);
        }
    }

    @Test
    public void testIterationColumnSelection() throws IOException {
        var batches = new int[]{2, 2, 1};
        var numRows = numRows(batches);
        var selection = Selection.all().retainColumns(1);

        try ( //
                var store = createData(batches); //
                var cursor = new DefaultColumnarCursor(store, selection) //
        ) {
            var access = cursor.access();
            assertEquals("size of the ReadAccessRow should be number of columns", 2, access.size());
            var intAccess = access.getAccess(0);
            var stringAccess = access.getAccess(1);
            var rowIdx = 0;
            assertTrue("should canForward", cursor.canForward());
            while (cursor.forward()) {
                assertThrows("should not be able to access column 0", Exception.class,
                    () -> ((IntReadAccess)intAccess).getIntValue());
                checkStringValue(rowIdx, stringAccess);
                rowIdx++;
                assertEquals("should canForward", rowIdx != numRows, cursor.canForward());
            }
            assertEquals("should read num rows", numRows, rowIdx);
        }
    }

    @Test
    public void testIterationRowSelection() throws IOException {
        var batches = new int[]{4, 4, 4, 2};
        var startRow = 5;
        var endRow = 11;
        var selection = Selection.all().retainRows(startRow, endRow);

        try ( //
                var store = createData(batches); //
                var cursor = new DefaultColumnarCursor(store, selection) //
        ) {
            var access = cursor.access();
            var intAccess = access.getAccess(0);
            var stringAccess = access.getAccess(1);
            var rowIdx = startRow;
            assertTrue("should canForward", cursor.canForward());
            while (cursor.forward()) {
                checkIntValue(rowIdx, intAccess);
                checkStringValue(rowIdx, stringAccess);
                rowIdx++;
                assertEquals("should canForward", rowIdx != endRow, cursor.canForward());
            }
            assertEquals("should read num rows", endRow, rowIdx);
        }
    }

    @Test
    public void testRandomAccessAll() throws IOException {
        var batches = new int[]{2, 2, 1};
        var rowsToTest = new int[]{4, 2, 3, 0, 4};

        try ( //
                var store = createData(batches); //
                var cursor = new DefaultColumnarCursor(store, Selection.all()) //
        ) {
            var access = cursor.access();
            assertEquals("size of the ReadAccessRow should be number of columns", 2, access.size());
            var intAccess = access.getAccess(0);
            var stringAccess = access.getAccess(1);

            for (var rowIdx : rowsToTest) {
                cursor.moveTo(rowIdx);
                checkIntValue(rowIdx, intAccess);
                checkStringValue(rowIdx, stringAccess);
            }
        }
    }

    @Test
    public void testRandomAccessColumnSelection() throws IOException {
        var batches = new int[]{2, 2, 1};
        var selection = Selection.all().retainColumns(1);
        var rowsToTest = new int[]{4, 2, 3, 0, 4};

        try ( //
                var store = createData(batches); //
                var cursor = new DefaultColumnarCursor(store, selection) //
        ) {
            var access = cursor.access();
            assertEquals("size of the ReadAccessRow should be number of columns", 2, access.size());
            var intAccess = access.getAccess(0);
            var stringAccess = access.getAccess(1);

            for (var rowIdx : rowsToTest) {
                cursor.moveTo(rowIdx);
                assertThrows("should not be able to access column 0", Exception.class,
                    () -> ((IntReadAccess)intAccess).getIntValue());
                checkStringValue(rowIdx, stringAccess);
            }
        }
    }

    @Test
    public void testRandomAccessRowSelection() throws IOException {
        var batches = new int[]{4, 4, 4, 2};
        var startRow = 5;
        var endRow = 11;
        var selection = Selection.all().retainRows(startRow, endRow);

        try ( //
                var store = createData(batches); //
                var cursor = new DefaultColumnarCursor(store, selection) //
        ) {
            var access = cursor.access();
            assertEquals("size of the ReadAccessRow should be number of columns", 2, access.size());
            var intAccess = access.getAccess(0);
            var stringAccess = access.getAccess(1);

            var rowIdx = 1;
            cursor.moveTo(rowIdx);
            checkIntValue(rowIdx + startRow, intAccess);
            checkStringValue(rowIdx + startRow, stringAccess);

            assertThrows("should throw outside of selection", IndexOutOfBoundsException.class, () -> cursor.moveTo(10));
            assertThrows("should throw outside of selection", IndexOutOfBoundsException.class,
                () -> cursor.moveTo(endRow - startRow));

            rowIdx = endRow - startRow - 1;
            cursor.moveTo(rowIdx);
            checkIntValue(rowIdx + startRow, intAccess);
            checkStringValue(rowIdx + startRow, stringAccess);
        }
    }

    @Test
    public void testEmptyBatches() throws IOException {
        try ( //
                var store = createData(0, 0); //
                var cursor = new DefaultColumnarCursor(store, Selection.all()) //
        ) {
            var access = cursor.access();
            assertEquals("size of the ReadAccessRow should be number of columns", 2, access.size());
            assertFalse("canForward should be false", cursor.canForward());
            assertFalse("forward should return false", cursor.forward());
            assertThrows("moveTo should throw", IndexOutOfBoundsException.class, () -> cursor.moveTo(0));
        }
    }

    private static TestBatchStore createData(final int... batchLength) throws IOException {
        var schema = ColumnarSchema.of(INT, STRING);
        var store = DefaultTestBatchStore.create(schema);

        // Fill the store with test data
        var runningInt = 0;
        try (var writer = store.getWriter()) {
            for (var l : batchLength) {
                var batch = writer.create(l);
                var intData = (TestIntData)batch.get(0);
                var stringData = (TestStringData)batch.get(1);

                for (var i = 0; i < l; i++) {
                    intData.setInt(i, runningInt);
                    stringData.setString(i, "" + runningInt);
                    runningInt++;
                }
                var readBatch = batch.close(l);
                writer.write(readBatch);
                readBatch.release();
            }
        }
        return store;
    }

    private static void checkIntValue(final int rowIdx, final ReadAccess access) {
        if (access instanceof IntReadAccess intAccess) {
            assertEquals("unexpected value", rowIdx, intAccess.getIntValue());
        } else {
            fail("must be int read access");
        }
    }

    private static void checkStringValue(final int rowIdx, final ReadAccess access) {
        if (access instanceof StringReadAccess stringAccess) {
            assertEquals("unexpected value", rowIdx + "", stringAccess.getStringValue());
        } else {
            fail("must be string read access");
        }
    }

    private static int numRows(final int... batchLength) {
        return IntStream.of(batchLength).sum();
    }

}
