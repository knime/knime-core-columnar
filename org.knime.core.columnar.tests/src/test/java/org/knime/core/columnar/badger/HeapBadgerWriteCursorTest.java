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
 *   20 Dec 2023 (chaubold): created
 */
package org.knime.core.columnar.badger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.columnar.testing.data.TestData;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.StringAccess.StringWriteAccess;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 *
 * @author chaubold
 */
@SuppressWarnings("javadoc")
public class HeapBadgerWriteCursorTest {

    //    @Test
    //    public void testWriteData() {
    //        HeapBadger badger = new HeapBadger(m_batchStore);
    //        WriteCursor<WriteAccessRow> cursor = badger.getWriteCursor();
    //
    //        var testData = getTestData();
    //
    //        for (int rowIdx = 0; rowIdx < testData.get(0).length(); rowIdx++) {
    //            var testReadAccessRow = new TestReadAccessRow(getTestData(), m_columnarSchema, rowIdx);
    //            cursor.access().setFrom(testReadAccessRow);
    //        }
    //
    //        cursor.close(); // ??
    //
    //        // Close to finish writing and to get the read cache?
    //        ObjectReadCache heapReadCache = badger.close();
    //
    //        // Check that all data that was written with the Cursor ended up in the store
    //        var writtenData = m_batchStore.getData();
    //        // compare testData to writtenData
    //        assertEquals(testData.size(), writtenData.size());
    //        for (int colIdx = 0; colIdx < testData.size(); colIdx++) {
    //            assertArrayEquals(testData.get(colIdx).get(), writtenData.get(colIdx).get());
    //        }
    //
    //        // TODO: check that everything is available in HeapCache
    //        //       by mocking the TestBatchStore so that we should not call it at all?
    //
    //    }

    @Test
    public void testWriteData2() throws IOException {
        ColumnarSchema columnarSchema = ColumnarSchema.of(INT, STRING);
        Integer[] intData = new Integer[]{1, 2, 3, 4};
        String[] stringData = new String[]{"A", "B", "C", "D"};
        final int numRows = intData.length;
        final int numCols = columnarSchema.numColumns();

        try (TestBatchStore batchStore = TestBatchStore.create(columnarSchema)) {
            HeapBadger badger = new HeapBadger(batchStore);
            try (WriteCursor<WriteAccessRow> cursor = badger.getWriteCursor()) {
                for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                    cursor.forward();
                    ((IntWriteAccess)cursor.access().getWriteAccess(0)).setIntValue(intData[rowIdx]);
                    ((StringWriteAccess)cursor.access().getWriteAccess(1)).setStringValue(stringData[rowIdx]);
                }
            }

            // Close to finish writing and to get the read cache?
            //        ObjectReadCache heapReadCache = badger.close();

            // Check that all data that was written with the Cursor ended up in the store
            var writtenData = batchStore.getData();

            // compare testData to writtenData
            assertEquals(numCols, writtenData.size());

            checkTestData(intData, writtenData.get(0));
            checkTestData(stringData, writtenData.get(1));

            // TODO: check that everything is available in HeapCache
            //       by mocking the TestBatchStore so that we should not call it at all?
        }
    }

    private static void checkTestData(final Object[] expected, final TestData data) {
        assertEquals(expected.length, data.length());
        assertArrayEquals(expected, Arrays.copyOf(data.get(), expected.length));
    }
}
