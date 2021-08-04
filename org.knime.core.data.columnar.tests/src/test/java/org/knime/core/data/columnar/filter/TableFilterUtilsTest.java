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
 *   Aug 2, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.filter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.data.container.filter.TableFilter;

/**
 * Contains unit tests for {@link TableFilterUtils}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class TableFilterUtilsTest {

    private static final TableFilter EMPTY = new TableFilter.Builder().build();

    @Test
    public void testDefinesRowRange() {
        TableFilter onlyFrom = TableFilter.filterRowsFromIndex(10);
        assertTrue(TableFilterUtils.definesRowRange(onlyFrom));
        TableFilter onlyTo = TableFilter.filterRowsToIndex(13);
        assertTrue(TableFilterUtils.definesRowRange(onlyTo));
        TableFilter range = TableFilter.filterRangeOfRows(10, 13);
        assertTrue(TableFilterUtils.definesRowRange(range));
        assertFalse(TableFilterUtils.definesRowRange(EMPTY));
    }

    @Test
    public void testExtractFrom() {
        TableFilter from = TableFilter.filterRowsFromIndex(10);
        assertEquals(10, TableFilterUtils.extractFromIndex(from));
        assertEquals(0, TableFilterUtils.extractFromIndex(EMPTY));
    }

    @Test
    public void testExtactTo() {
        TableFilter to = TableFilter.filterRowsToIndex(12);
        assertEquals(12, TableFilterUtils.extractToIndex(to, 100));
        assertEquals(99, TableFilterUtils.extractToIndex(EMPTY, 100));
    }

    @Test
    public void testDefinesColumnFilter() {
        TableFilter filter = TableFilter.materializeCols(0, 2);
        assertTrue(TableFilterUtils.definesColumnFilter(filter));
        assertFalse(TableFilterUtils.definesColumnFilter(EMPTY));
    }

    @Test
    public void testExtractPhysicalColumnIndices() {
        TableFilter filter = TableFilter.materializeCols(0, 2);
        int[] expected = {0, 1, 3};
        assertArrayEquals(expected, TableFilterUtils.extractPhysicalColumnIndices(filter, 5));
        expected = new int[] {0, 1, 2, 3, 4};
        assertArrayEquals(expected, TableFilterUtils.extractPhysicalColumnIndices(EMPTY, 5));
    }

    @Test
    public void testHasFilter() {
        TableFilter onlyCols = TableFilter.materializeCols(1, 2, 3);
        assertTrue(TableFilterUtils.hasFilter(onlyCols));
        TableFilter onlyFromIndex = TableFilter.filterRowsFromIndex(3);
        assertTrue(TableFilterUtils.hasFilter(onlyFromIndex));
        TableFilter onlyToIndex = TableFilter.filterRowsToIndex(7);
        assertTrue(TableFilterUtils.hasFilter(onlyToIndex));
        TableFilter fromAndToIndex = TableFilter.filterRangeOfRows(3, 7);
        assertTrue(TableFilterUtils.hasFilter(fromAndToIndex));
        TableFilter noFilter = new TableFilter.Builder().build();
        assertFalse(TableFilterUtils.hasFilter(noFilter));
    }

    @Test
    public void testCreateColumnSelectionAllColumnsFiltered() {
        TableFilter filter = TableFilter.materializeCols();
        Optional<ColumnSelection> selection = TableFilterUtils.createColumnSelection(filter, 5);
        assertTrue(selection.isPresent());
        checkSelection(selection.get(), 5, 0);
    }

    private static void checkSelection(final ColumnSelection selection, final int expectedSize,
        final int... selectedIndices) {
        assertEquals(expectedSize, selection.numColumns());
        Set<Integer> selected = Arrays.stream(selectedIndices).boxed().collect(Collectors.toSet());
        for (int i = 0; i < expectedSize; i++) {
            assertEquals(selected.contains(i), selection.isSelected(i));
        }
    }

    @Test
    public void testCreateColumnSelectionSomeColumnsFiltered() {
        TableFilter filter = TableFilter.materializeCols(0, 2, 4);
        Optional<ColumnSelection> selection = TableFilterUtils.createColumnSelection(filter, 6);
        assertTrue(selection.isPresent());
        checkSelection(selection.get(), 6, 0, 1, 3, 5);
    }

    @Test
    public void testCreateColumnSelectionNoColumnsFiltered() throws Exception {
        TableFilter filter = new TableFilter.Builder().build();
        Optional<ColumnSelection> selection = TableFilterUtils.createColumnSelection(filter, 5);
        assertTrue(selection.isEmpty());
    }
}
