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

import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.table.row.Selection;

/**
 * Utility class for dealing with the {@link TableFilter} API.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class TableFilterUtils {

    /**
     * Get the {@link Selection} corresponding to the given {@link TableFilter}.
     * <p>
     * {@code TableFilter} are used in the {@code BufferedDataTable} API, where the row key column is always present and
     * is not included in the column count. {@code Selection} is used in the {@code RowAccessible} API, where the row
     * key column is not treated separately and is just column 0. Therefore, column index {@code i} in the
     * {@code TableFilter} becomes column index {@code i+1} in the {@code Selection}, and the returned {@code Selection}
     * always contains column {@code 0} (the row key).
     * <p>
     * {@code TableFilter} may express an open row range, where all rows are retained, starting from a given index.
     * {@code Selection} cannot express this. It includes either all rows, or a closed row range. The {@code numRows}
     * argument is used to determine the upper bound of the row range of the {@code Selection}, if the
     * {@code TableFilter} contains an open row range with only the start index given. (Otherwise, {@code numRows} is
     * ignored).
     *
     * @param filter the selected columns and row range
     * @param numRows the number of rows in the table
     * @return the {@code Selection} that is equivalent to {@code filter}
     */
    public static Selection toSelection(final TableFilter filter, final long numRows) {
        Selection selection = Selection.all();
        final Optional<Set<Integer>> columnIndices = filter.getMaterializeColumnIndices();
        if (columnIndices.isPresent()) {
            final int[] cols = IntStream.concat( //
                IntStream.of(0), // the row key column is always included and therefore not subject to TableFilter
                columnIndices.get().stream().mapToInt(i -> i + 1) // adjust for the row key column
            ).toArray();
            selection = selection.retainColumns(cols);
        }
        final Optional<Long> fromRowIndex = filter.getFromRowIndex();
        final Optional<Long> toRowIndex = filter.getToRowIndex();
        if (fromRowIndex.isPresent() || toRowIndex.isPresent()) {
            final long fromIndex = fromRowIndex.orElse(0L);
            final long toIndex = toRowIndex.orElse(numRows - 1) + 1; // TableFilter toRowIndex is inclusive, Selection toIndex is exclusive
            selection = selection.retainRows(fromIndex, toIndex);
        }
        return selection;
    }

    private TableFilterUtils() {
        // static utility class
    }
}
