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
import static org.junit.Assert.assertTrue;
import static org.knime.core.data.columnar.table.ColumnarTableTestUtils.createUnsavedColumnarContainerTable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.IntValue;
import org.knime.core.data.UnmaterializedCell;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.node.ExtensionTable;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarRowIteratorTest extends ColumnarTest {

    static void compare(final ExtensionTable table, final int... values) {
        int nCols = table.getDataTableSpec().getNumColumns();
        try (final CloseableRowIterator iterator = table.iterator()) {
            DataRow row = null;
            for (final int value : values) {
                assertTrue(iterator.hasNext());
                row = iterator.next();
                assertNotNull(row);
                Assert.assertTrue(nCols == row.getNumCells());
                final Iterator<DataCell> cellIterator = row.iterator();
                assertTrue(cellIterator.hasNext());
                assertEquals(Integer.toString(value), row.getKey().getString());
                for (int j = 0; j < nCols; j++) {
                    if (value % 2 == 0) { // NOSONAR
                        assertTrue(cellIterator.next().isMissing());
                    } else {
                        assertEquals(value, ((IntValue)cellIterator.next()).getIntValue());
                    }
                }
                assertFalse(cellIterator.hasNext());
                try {
                    cellIterator.next();
                    Assert.fail();
                } catch (NoSuchElementException e) { // NOSONAR
                }
            }
            assertFalse(iterator.hasNext());
            try {
                iterator.next();
                Assert.fail();
            } catch (NoSuchElementException e) { // NOSONAR
            }
        }
    }

    private static void compare(final ExtensionTable table, final TableFilter filter, final int... values) { // NOSONAR
        int nCols = table.getDataTableSpec().getNumColumns();
        final Set<Integer> indices = filter.getMaterializeColumnIndices()
            .orElse(IntStream.range(0, table.getDataTableSpec().getNumColumns()).boxed().collect(Collectors.toSet()));
        try (final CloseableRowIterator iterator = table.iteratorWithFilter(filter)) {
            DataRow row = null;
            for (final int value : values) {
                assertTrue(iterator.hasNext());
                row = iterator.next();
                assertNotNull(row);
                Assert.assertTrue(nCols == row.getNumCells());
                final Iterator<DataCell> cellIterator = row.iterator();
                assertEquals(Integer.toString(value), row.getKey().getString());
                while (cellIterator.hasNext()) {
                    if (value % 2 == 0) { // NOSONAR
                        assertTrue(cellIterator.next().isMissing());
                    } else {
                        assertEquals(value, ((IntValue)cellIterator.next()).getIntValue());
                    }
                }
                try {
                    cellIterator.next();
                    Assert.fail();
                } catch (NoSuchElementException e) { // NOSONAR
                }
                for (int j : indices) {
                    if (value % 2 == 0) { // NOSONAR
                        assertTrue(row.getCell(j).isMissing());
                    } else {
                        assertEquals(value, ((IntValue)(row.getCell(j))).getIntValue());
                    }
                }
            }
            assertFalse(iterator.hasNext());
            try {
                iterator.next();
                Assert.fail();
            } catch (NoSuchElementException e) { // NOSONAR
            }
        }
    }

    private static final UnmaterializedCell UNMATERIALZED_CELL = UnmaterializedCell.getInstance();

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
    public void testDefaultIterator() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2)) {
            compare(table, 0, 1);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDefaultIteratorRemoveUnsupported() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2);
                final CloseableRowIterator iterator = table.iterator()) {
            iterator.next().iterator().remove();
        }
    }

    @Test
    public void testSingleCellFilteredRowIterator() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2)) {
            compare(table, TableFilter.materializeCols(0), 0, 1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSingleCellFilteredRowIteratorGetCellIndexOutOfBoundsLower() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2);
                final CloseableRowIterator iterator = table.iteratorWithFilter(TableFilter.materializeCols(0))) {
            iterator.next().getCell(-1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSingleCellFilteredRowIteratorGetCellIndexOutOfBoundsUpper() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2);
                final CloseableRowIterator iterator = table.iteratorWithFilter(TableFilter.materializeCols(0))) {
            iterator.next().getCell(1);
        }
    }

    @Test
    public void testSingleCellFilteredRowIteratorGetUnmaterializedCell() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2, 2);
                final CloseableRowIterator iterator = table.iteratorWithFilter(TableFilter.materializeCols(0))) {
            assertEquals(UNMATERIALZED_CELL, iterator.next().getCell(1));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSingleCellFilteredRowIteratorRemoveUnsupported() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2, 2);
                final CloseableRowIterator iterator = table.iteratorWithFilter(TableFilter.materializeCols(0))) {
            iterator.next().iterator().remove();
        }
    }

    @Test
    public void testArrayFilteredRowIterator() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(3, 2)) {
            compare(table, TableFilter.materializeCols(1, 2), 0, 1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testArrayFilteredRowIteratorGetCellIndexOutOfBoundsLower() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(3, 2);
                final CloseableRowIterator iterator = table.iteratorWithFilter(TableFilter.materializeCols(1, 2))) {
            iterator.next().getCell(-1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testArrayFilteredRowIteratorGetCellIndexOutOfBoundsUpper() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(3, 2);
                final CloseableRowIterator iterator = table.iteratorWithFilter(TableFilter.materializeCols(1, 2))) {
            iterator.next().getCell(3);
        }
    }

    @Test
    public void testArrayFilteredRowIteratorGetUnmaterializedCell() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(3, 2);
                final CloseableRowIterator iterator = table.iteratorWithFilter(TableFilter.materializeCols(1, 2))) {
            assertEquals(UNMATERIALZED_CELL, iterator.next().getCell(0));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testArrayFilteredRowIteratorRemoveUnsupported() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(3, 2);
                final CloseableRowIterator iterator = table.iteratorWithFilter(TableFilter.materializeCols(1, 2))) {
            iterator.next().iterator().remove();
        }
    }

    @Test
    public void testHashMapFilteredRowIterator() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(100, 2)) {
            compare(table, TableFilter.materializeCols(IntStream.range(20, 80).toArray()), 0, 1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testHashMapFilteredRowIteratorGetCellIndexOutOfBoundsLower() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(100, 2);
                final CloseableRowIterator iterator =
                    table.iteratorWithFilter(TableFilter.materializeCols(IntStream.range(20, 80).toArray()))) {
            iterator.next().getCell(-1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testHashMapFilteredRowIteratorGetCellIndexOutOfBoundsUpper() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(100, 2);
                final CloseableRowIterator iterator =
                    table.iteratorWithFilter(TableFilter.materializeCols(IntStream.range(20, 80).toArray()))) {
            iterator.next().getCell(100);
        }
    }

    @Test
    public void testHashMapFilteredRowIteratorGetUnmaterializedCell() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(100, 2);
                final CloseableRowIterator iterator =
                    table.iteratorWithFilter(TableFilter.materializeCols(IntStream.range(20, 80).toArray()))) {
            assertEquals(UNMATERIALZED_CELL, iterator.next().getCell(0));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testHashMapFilteredRowIteratorRemoveUnsupported() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(100, 2);
                final CloseableRowIterator iterator =
                    table.iteratorWithFilter(TableFilter.materializeCols(IntStream.range(20, 80).toArray()))) {
            iterator.next().iterator().remove();
        }
    }

}
