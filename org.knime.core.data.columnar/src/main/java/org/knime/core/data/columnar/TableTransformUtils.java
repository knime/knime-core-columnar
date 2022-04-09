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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.append.AppendedRowsTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Utility class that contains various methods for dealing with TableTransforms in ColumnarTableBackend.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class TableTransformUtils {

    private static final TableFilter ONLY_ROWKEYS = TableFilter.materializeCols();

    static DataTableSpec concatenateSpec(final BufferedDataTable[] tables) {
        return AppendedRowsTable.generateDataTableSpec(extractSpecs(tables));
    }

    private static DataTableSpec[] extractSpecs(final BufferedDataTable[] referenceTables) {
        return Stream.of(referenceTables)//
            .map(DataTable::getDataTableSpec)//
            .toArray(DataTableSpec[]::new);
    }

    private static DataTableSpec[] extractSpecs(final ReferenceTable[] referenceTables) {
        return Stream.of(referenceTables)//
                .map(ReferenceTable::getBufferedTable)//
                .map(DataTable::getDataTableSpec)//
                .toArray(DataTableSpec[]::new);
    }

    static DataTableSpec appendSpec(final BufferedDataTable[] tables) {
        return createAppendSpec(extractSpecs(tables));
    }

    private static DataTableSpec createAppendSpec(final DataTableSpec[] specs) {
        final var creator = new DataTableSpecCreator(specs[0]);
        for (var i = 1; i < specs.length; i++) {
            creator.addColumns(specs[i]);
        }
        return creator.createSpec();
    }

    static long concatenatedSize(final BufferedDataTable[] tables) {
        return sizeStream(tables).sum();
    }

    private static LongStream sizeStream(final BufferedDataTable[] tables) {
        return Arrays.stream(tables).mapToLong(BufferedDataTable::size);
    }

    static long appendSize(final BufferedDataTable[] tables) {
        CheckUtils.checkArgument(sizeStream(tables).distinct().count() == 1,
            "Tables can't be joined, non matching row counts");
        return tables[0].size();
    }

    static VirtualTable appendTables(final ReferenceTable[] tables) {
        var virtualTable = new VirtualTable(tables[0].getId(), tables[0].getSchema());
        var appendedTables = Stream.of(tables)//
                .skip(1)//
                .map(TableTransformUtils::asSource)//
                .map(TableTransformUtils::filterRowKey)//
                .collect(toList());
        return virtualTable.append(appendedTables);
    }

    private static VirtualTable filterRowKey(final VirtualTable table) {
        return table.filterColumns(//
            IntStream.range(1, table.getSchema().numColumns())//
                .toArray()//
        );
    }

    private static VirtualTable asSource(final ReferenceTable table) {
        return new VirtualTable(table.getId(), table.getSchema());
    }

    static List<TableTransformSpec> createAppendTransformations(final DataTableSpec[] specs) {
        final var selection = filterOutRedundantRowKeyColumns(specs);
        return List.of(new AppendTransformSpec(), new ColumnFilterTransformSpec(selection));
    }

    private static int[] filterOutRedundantRowKeyColumns(final DataTableSpec[] specs) {
        final var totalColumns = Arrays.stream(specs).mapToInt(DataTableSpec::getNumColumns).sum() + 1;
        final var selection = new int[totalColumns];
        var idx = 1;
        var filteredIdx = 1;
        for (DataTableSpec spec : specs) {
            for (var i = 0; i < spec.getNumColumns(); i++) {
                selection[idx] = filteredIdx;
                idx++;
                filteredIdx++;
            }
            filteredIdx++;
        }
        return selection;
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

    static void checkForDuplicateKeys(final ExecutionMonitor progMon, final BufferedDataTable[] tables) {
        final var checker = new DuplicateChecker();
        final var progress = new Progress(progMon, concatenatedSize(tables));
        try {
            checkForDuplicateKeys(progress, checker, tables);
        } finally {
            checker.clear();
        }
    }

    private static void checkForDuplicateKeys(final Progress progress, final DuplicateChecker checker,
        final BufferedDataTable[] tables) {
        for (var i = 0; i < tables.length; i++) {
            try {
                addKeys(progress, checker, tables[i]);
            } catch (DuplicateKeyException ex) {
                throw new IllegalArgumentException("Duplicate row key \"" + ex.getKey() + "\" in table with index " + i,
                    ex);
            } catch (IOException ex) {
                throw new IllegalArgumentException("An I/O problem occurred while checking for duplicate keys.", ex);
            }
        }
        try {
            checker.checkForDuplicates();
        } catch (DuplicateKeyException | IOException ex) {
            throw new IllegalArgumentException("Duplicate row keys", ex);
        }
    }

    private static void addKeys(final Progress progress, final DuplicateChecker checker, final BufferedDataTable table)
        throws DuplicateKeyException, IOException {
        try (RowCursor cursor = table.cursor(ONLY_ROWKEYS)) {
            while (cursor.canForward()) {
                final var key = cursor.forward().getRowKey().getString();
                checker.addKey(key);
                progress.report(key);
            }
        }
    }

    private static final class Progress {
        private final ExecutionMonitor m_progressMonitor;

        private final long m_totalSize;

        private final double m_doubleSize;

        private long m_currentRow = 0;

        Progress(final ExecutionMonitor progressMonitor, final long totalSize) {
            m_progressMonitor = progressMonitor;
            m_totalSize = totalSize;
            m_doubleSize = totalSize;
        }

        void report(final String key) {
            m_progressMonitor.setProgress(m_currentRow / m_doubleSize,
                () -> String.format("Checking tables, row %d/%d ('%s')", m_currentRow, m_totalSize, key));
        }

    }

    /**
     * Applies the combined filter and permutation defined by <b>originalIndices</b> to the table.
     * The filter and permutation are applied as separate operations.
     *
     * @param table the input table
     * @param originalIndices the index is the position in the output and the value the index in the original table
     * @return the filtered and permuted table
     */
    public static VirtualTable filterAndPermute(final VirtualTable table, final int[] originalIndices) {
        // -1 because table contains a row key column that is not included in originalIndices
        final int originalNumColumns = table.getSchema().numColumns() - 1;
        final var properties = new IndexArrayProperties(originalIndices, originalNumColumns);
        if (properties.m_isComplete && properties.m_isSorted) {
            return table;
        } else if (properties.m_isComplete) {
            return table.permute(prependRowKeyColumnIndex(originalIndices));
        } else if (properties.m_isSorted) {
            return table.filterColumns(prependRowKeyColumnIndex(originalIndices));
        } else {
            return splitIntoFilterAndPermute(table, originalIndices);
        }
    }

    private static VirtualTable splitIntoFilterAndPermute(final VirtualTable sourceTable, final int[] originalIndices) {
        final int[] filter = Arrays.stream(originalIndices).sorted().toArray();
        final TIntIntMap offsets = new TIntIntHashMap();
        for (int selected : filter) {
            offsets.put(selected, selected - offsets.size());
        }
        final int[] permutation = Arrays.stream(originalIndices)//
            .map(i -> i - offsets.get(i))//
            .toArray();
        return sourceTable//
                .filterColumns(prependRowKeyColumnIndex(filter))//
                .permute(prependRowKeyColumnIndex(permutation));
    }

    private static int[] prependRowKeyColumnIndex(final int[] indices) {
        return IntStream.concat(//
            IntStream.of(0), // the row key is always included
            Arrays.stream(indices).map(i -> i + 1) // shift to accommodate the row key
        ).toArray();
    }

    public static final class IndexArrayProperties {

        private final boolean m_isSorted;

        private final boolean m_isComplete;

        public IndexArrayProperties(final int[] indices, final int originalNumColumns) {
            if (indices.length == 0) {
                m_isSorted = true;
                m_isComplete = originalNumColumns == 0;
            } else {
                var isSorted = checkSortedAndUnique(indices);
                m_isComplete = indices.length == originalNumColumns;
                m_isSorted = isSorted;
            }
        }

        private static boolean checkSortedAndUnique(final int[] indices) {
            var isSorted = true;
            var previous = indices[0];
            final TIntSet uniqueIndices = new TIntHashSet(indices.length);
            uniqueIndices.add(previous);
            for (var i = 1; i < indices.length; i++) {
                var current = indices[i];
                if (current < previous) {
                    isSorted = false;
                }
                CheckUtils.checkArgument(uniqueIndices.add(current),
                    "The provided indices contain the index %s multiple times.", current);
                previous = current;
            }
            return isSorted;
        }
        public boolean isSorted() {
            return m_isSorted;
        }

        public boolean isComplete() {
            return m_isComplete;
        }
    }


    private TableTransformUtils() {
        // static utility class
    }

}
