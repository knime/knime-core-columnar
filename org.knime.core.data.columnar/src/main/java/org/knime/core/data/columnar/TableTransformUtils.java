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
 *   Jul 23, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.virtual.VirtualTable;

/**
 * Utility class that contains various methods for dealing with TableTransforms in ColumnarTableBackend.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class TableTransformUtils {

    private static LongStream sizeStream(final BufferedDataTable[] tables) {
        return Arrays.stream(tables).mapToLong(BufferedDataTable::size);
    }

    static long appendSize(final BufferedDataTable[] tables) {
        CheckUtils.checkArgument(sizeStream(tables).distinct().count() == 1,
            "Tables can't be joined, non matching row counts");
        return tables[0].size();
    }

    static VirtualTable appendTables(final ReferenceTable[] tables, final int rowIDTable) {
        var virtualTable = asSource(tables[0]);
        var numColumnsInFirstTable = numColumns(virtualTable);
        var appendedTables = new ArrayList<VirtualTable>(tables.length - 1);
        int rowIDColumnIndex = 0;//NOSONAR
        for (int i = 1; i < tables.length; i++) {//NOSONAR
            var appendedTable = asSource(tables[i]);
            if (i != rowIDTable) {
                appendedTable = filterRowKey(appendedTable);
                if (i < rowIDTable) {
                    rowIDColumnIndex += numColumns(appendedTable);
                }
            }
            appendedTables.add(appendedTable);
        }

        virtualTable = virtualTable.append(appendedTables);
        if (rowIDTable > 0) {
            virtualTable = filterRowKey(virtualTable);
            rowIDColumnIndex += numColumnsInFirstTable - 1;
            final int finalRowIDCol = rowIDColumnIndex;
            virtualTable = virtualTable.selectColumns(//
                IntStream.concat(//
                    IntStream.of(rowIDColumnIndex), // move RowID column first
                    IntStream.range(0, numColumns(virtualTable)) // fill in remaining columns
                    .filter(i -> i != finalRowIDCol) // ignore rowID column index
                ).toArray());
        }
        return virtualTable;
    }

    private static int numColumns(final VirtualTable virtualTable) {
        return virtualTable.getSchema().numColumns();
    }

    private static VirtualTable filterRowKey(final VirtualTable table) {
        return table.dropColumns(0);
    }

    private static VirtualTable asSource(final ReferenceTable table) {
        return new VirtualTable(table.getId(), table.getSchema(), true);
    }

    static void checkRowKeysMatch(final ExecutionMonitor exec, final BufferedDataTable... tables)
        throws CanceledExecutionException {
        checkAllSameSize(tables);
        final var filter = TableFilter.materializeCols();
        final RowCursor[] cursors = Arrays.stream(tables)//
            .map(t -> t.cursor(filter))//
            .toArray(RowCursor[]::new);
        try {
            @SuppressWarnings("resource") // closed in the finally block
            final var leadCursor = cursors[0];
            final var longSize = tables[0].size();
            final double size = longSize;
            // all tables have the same length so we only need to check one cursor
            for (long rowIndex = 0; leadCursor.canForward(); rowIndex++) {
                exec.checkCanceled();
                final var rowKey = cursors[0].forward().getRowKey().getString();
                checkRowKeysMatch(rowKey, cursors, rowIndex);
                final long finalRowIndex = rowIndex;
                exec.setProgress(rowIndex / size, () -> String.format("'%s' (%d/%d)", rowKey, finalRowIndex, longSize));
            }
        } finally {
            Arrays.stream(cursors).forEach(RowCursor::close);
        }
    }

    static void checkAllSameSize(final BufferedDataTable... tables) {
        final var size = tables[0].size();
        for (var i = 1; i < tables.length; i++) {
            if (size != tables[i].size()) {
                throw new IllegalArgumentException("Not all tables have the same number of rows.");
            }
        }
    }

    private static void checkRowKeysMatch(final String firstKey, final RowCursor[] cursors, final long rowIndex) {
        for (var i = 1; i < cursors.length; i++) {
            final var otherKey = cursors[i].forward().getRowKey().getString();
            if (!firstKey.equals(otherKey)) {
                throw new IllegalArgumentException(
                    "Tables contain non-matching rows or are sorted differently, keys in row " + rowIndex
                        + " do not match: \"" + firstKey + "\" vs. \"" + otherKey + "\"");
            }
        }
    }




    private TableTransformUtils() {
        // static utility class
    }

}
