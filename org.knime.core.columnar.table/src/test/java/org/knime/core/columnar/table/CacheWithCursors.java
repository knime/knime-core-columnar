package org.knime.core.columnar.table;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.knime.core.columnar.table.ColumnarTableTestUtils.TestDoubleColumnType.TestDoubleReadValue;
import org.knime.core.columnar.table.ColumnarTableTestUtils.TestDoubleColumnType.TestDoubleWriteValue;

public class CacheWithCursors {

	@Test
	public void testCache() throws Exception {
		int numRows = 1337;
		int numColumns = 32;
		int chunkSize = 37;

		final WriteTable wTable = ColumnarTableTestUtils.createWriteTableWithCache(numColumns, chunkSize);
		try (TableWriteCursor wCursor = wTable.getCursor()) {
			for (int i = 0; i < numRows; i++) {
				wCursor.fwd();
				for (int j = 0; j < numColumns; j++) {
					TestDoubleWriteValue wValue = wCursor.get(j);
					wValue.setDouble(j * i);
				}
			}
		}

		// create read table
		try (ReadTable rTable = wTable.createReadTable(); TableReadCursor rCursor = rTable.cursor()) {
			int i = 0;
			while (rCursor.canFwd()) {
				rCursor.fwd();
				for (int j = 0; j < numColumns; j++) {
					TestDoubleReadValue rValue = rCursor.get(j);
					assertEquals(j * i, rValue.getDouble(), 0);
				}
				i++;
			}
		}
	}
}
