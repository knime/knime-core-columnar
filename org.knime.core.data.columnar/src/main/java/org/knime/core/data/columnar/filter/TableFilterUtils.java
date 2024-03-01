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

import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.table.row.Selection;

/**
 * Utility class for dealing with the {@link TableFilter} API.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class TableFilterUtils {

    /**
     * Checks if the provided {@link TableFilter} actually filters any columns or rows.
     *
     * @param filter to check
     * @return {@code true} if either columns or rows are filtered
     */
    public static boolean hasFilter(final TableFilter filter) {
        return filter.getMaterializeColumnIndices().isPresent() || filter.getFromRowIndex().isPresent()
            || filter.getToRowIndex().isPresent();
    }

    /**
     * Checks if the TableFilter defines a row range i.e. at least one of {@link TableFilter#getFromRowIndex()} and
     * {@link TableFilter#getToRowIndex()} returns a non-empty Optional.
     *
     * @param filter to check
     * @return {@code true} if the TableFilter defines a range of rows
     */
    public static boolean definesRowRange(final TableFilter filter) {
        return filter.getFromRowIndex().isPresent() || filter.getToRowIndex().isPresent();
    }

    /**
     * Returns the index to start iterating at.
     *
     * @param filter to extract the from index from
     * @return the from index defined in the TableFilter or 0 if no from index is specified
     */
    public static long extractFromIndex(final TableFilter filter) {
        return filter.getFromRowIndex().orElse(0L);
    }

    /**
     * Returns the index to stop iterating at.
     *
     * @param filter to extract the to index from
     * @param size number of rows in the table that is filtered
     * @return the to index defined in TableFilter or {@code size - 1} if no to index is specified
     */
    public static long extractToIndex(final TableFilter filter, final long size) {
        return filter.getToRowIndex().orElse(size - 1);
    }

    /**
     * Checks if the TableFilter defines a column filter.
     *
     * @param filter to check
     * @return {@code true} if the TableFilter defines a column filter
     */
    public static boolean definesColumnFilter(final TableFilter filter) {
        return filter.getMaterializeColumnIndices().isPresent();
    }

    /**
     * Extracts the column filter indices from TableFilter. The indices are sorted and incremented by 1 because the row
     * key is not considered to be a column in KNIME but is a column in fast tables.
     *
     * @param filter to extract the column filter from
     * @param numColumns the number of columns in the fast table
     * @return the column filter defined in the TableFilter adapted to fast tables, or all indices if TableFilter
     *         doesn't define a column filter
     */
    public static int[] extractPhysicalColumnIndices(final TableFilter filter, final int numColumns) {
        return filter.getMaterializeColumnIndices()//
            .map(TableFilterUtils::toSortedIntArrayWithRowKey) // extract filter
            .orElseGet(() -> IntStream.range(0, numColumns).toArray());// no filter -> include all column
    }

    private static int[] toSortedIntArrayWithRowKey(final Set<Integer> materializedColumns) {
        return IntStream.concat(IntStream.of(0), // the first column is the row key and has to always be included
            materializedColumns.stream()//
                .mapToInt(Integer::intValue)//
                .sorted()//
                .map(i -> i + 1))// the table filter doesn't include the row key as first column
            .toArray();
    }

    /**
     * Creates a {@link ColumnSelection} based on the provided {@link TableFilter}.
     *
     * @param filter to extract the {@link ColumnSelection} from
     * @param numColumns number of columns in the table the filter is applied to
     * @return an {@link Optional} of the {@link ColumnSelection} contained in {@link TableFilter} or
     *         {@link Optional#empty()} if the TableFilter doesn't filter any columns
     */
    public static Optional<ColumnSelection> createColumnSelection(final TableFilter filter, final int numColumns) {
        return filter.getMaterializeColumnIndices()//
            .map(TableFilterUtils::toSortedIntArrayWithRowKey)//
            .map(i -> new FilteredColumnSelection(numColumns, i));
    }

    /**
     * Create a {@link Selection} based on the provided {@link TableFilter}.
     *
     * @param filter to extract the {@link Selection} from
     * @param numColumns number of columns in the table the filter is applied to
     * @param numRows number of rows in the table that is filtered
     * @return the Selection equivalent to the {@link TableFilter}
     */
    public static Selection createSelection(final TableFilter filter, final int numColumns, final long numRows) {
        // Columns
        final var columns = definesColumnFilter(filter)
            ? Selection.ColumnSelection.all().retain(extractPhysicalColumnIndices(filter, numColumns))
            : Selection.ColumnSelection.all();

        // Rows
        final var rows = Selection.RowRangeSelection.all().retain( //
            /* from */ extractFromIndex(filter), //
            /* to   */ extractToIndex(filter, numRows) + 1 //
        );

        return Selection.all().retainColumns(columns).retainRows(rows);
    }

    private TableFilterUtils() {
        // static utility class
    }

}
