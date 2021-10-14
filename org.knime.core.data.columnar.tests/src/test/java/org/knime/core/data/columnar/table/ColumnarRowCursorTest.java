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
 *   29 Jan 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.knime.core.data.columnar.table.ColumnarTableTestUtils.createUnsavedColumnarContainerTable;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.value.ValueInterfaces.IntReadValue;
import org.knime.core.node.ExtensionTable;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarRowCursorTest extends ColumnarTest {

    private static final int CAPACITY = ColumnarRowWriteCursor.CAPACITY_MAX_DEF;

    static void compare(final ExtensionTable table, final int... values) {
        int nCols = table.getDataTableSpec().getNumColumns();
        try (final RowCursor cursor = table.cursor()) {
            Assert.assertTrue(nCols <= cursor.getNumColumns());
            RowRead row = null;
            for (final int value : values) {
                assertTrue(cursor.canForward());
                row = cursor.forward();
                assertNotNull(row);
                assertEquals(Integer.toString(value), row.getRowKey().getString());
                for (int j = 0; j < nCols; j++) {
                    if (value % 2 == 0) { // NOSONAR
                        assertTrue(row.isMissing(j));
                    } else {
                        assertEquals(value, row.<IntReadValue> getValue(j).getIntValue());
                    }
                }
            }
            assertFalse(cursor.canForward());
            assertNull(cursor.forward());
        }
    }

    private static void compare(final ExtensionTable table, final TableFilter filter, final int... values) {
        final Set<Integer> indices = filter.getMaterializeColumnIndices().orElseGet(
            () -> IntStream.range(0, table.getDataTableSpec().getNumColumns()).boxed().collect(Collectors.toSet()));
        try (final RowCursor cursor = table.cursor(filter)) {
            RowRead row = null;
            for (final int value : values) {
                assertTrue(cursor.canForward());
                row = cursor.forward();
                assertNotNull(row);
                assertEquals(Integer.toString(value), row.getRowKey().getString());
                for (int j : indices) {
                    if (value % 2 == 0) { // NOSONAR
                        assertTrue(row.isMissing(j));
                    } else {
                        assertEquals(value, row.<IntReadValue> getValue(j).getIntValue());
                    }
                }
            }
            assertFalse(cursor.canForward());
            assertNull(cursor.forward());
        }
    }

    @Test
    public void testEmptyRowCursorOnEmptyTable() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0)) {
            compare(table);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIndexOutOfBoundsFromIndex() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0)) {
            compare(table, TableFilter.filterRowsFromIndex(1));
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIndexOutOfBoundsToIndex() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0)) {
            compare(table, TableFilter.filterRowsToIndex(1));
        }
    }

    @Test
    public void testSingleBatchRowCursorOnSingleBatchTable() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2)) {
            compare(table, TableFilter.filterRangeOfRows(0, 1), 0, 1);
        }
    }

    @Test
    public void testSingleBatchRowCursorOnMultiBatchTable() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(CAPACITY + 2)) {
            compare(table, TableFilter.filterRangeOfRows(CAPACITY, CAPACITY + 1L), CAPACITY, CAPACITY + 1);
        }
    }

    @Test
    public void testSingleBatchRowCursorWithSelection() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2)) {
            compare(table,
                new TableFilter.Builder().withFromRowIndex(0).withToRowIndex(1).withMaterializeColumnIndices(0).build(),
                0, 1);
        }
    }

    @Test
    public void testMultiBatchRowCursor() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(CAPACITY + 2)) {
            compare(table, TableFilter.filterRangeOfRows(CAPACITY - 1L, CAPACITY), CAPACITY - 1, CAPACITY);
        }
    }

    @Test
    public void testMultiBatchRowCursorWithSelection() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(CAPACITY + 2)) {
            compare(table, new TableFilter.Builder().withFromRowIndex(CAPACITY - 1L).withToRowIndex(CAPACITY)
                .withMaterializeColumnIndices(0).build(), CAPACITY - 1, CAPACITY);
        }
    }

}
