package org.knime.core.columnar.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.knime.core.columnar.table.TestColumnarTableUtils.TestDoubleColumnType.TestDoubleReadValue;
import org.knime.core.columnar.table.TestColumnarTableUtils.TestDoubleColumnType.TestDoubleWriteValue;

public class CursorTests {

	@Test
	public void testIdentity() throws Exception {
		int numRows = 107;
		int numColumns = 1;
		int chunkSize = 10;

		WriteTable wTable = TestColumnarTableUtils.createWriteTable(numColumns, chunkSize);
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

		// create read table
		ReadTable rTable = wTable.createReadTable();

		assertEquals(numColumns, rTable.getNumColumns());
		assertEquals(numColumns, rTable.getSchema().getNumColumns());

		// test cursor
		try (TableReadCursor rCursor = rTable.cursor()) {
			readIdentity(0, numRows - 1, rCursor);
		}

		// test cursor with filter which doesn't actually filter
		try (TableReadCursor rCursor = rTable
				.cursor(TestColumnarTableUtils.createTableReadCursorConfig(0, numRows - 1, 0))) {
			readIdentity(0, numRows - 1, rCursor);
		}

		// test cursor with filter from 5-10
		try (TableReadCursor rCursor = rTable.cursor(TestColumnarTableUtils.createTableReadCursorConfig(42, 99, 0))) {
			readIdentity(42, 99, rCursor);
		}

		rTable.close();

	}

	private void readIdentity(int firstIndex, int lastIndex, TableReadCursor rCursor) throws Exception {
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
