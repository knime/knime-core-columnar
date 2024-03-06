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
 *   Mar 11, 2024 (leonard.woerteler): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.ColumnarTableBackend;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.virtual.VirtualTableUtils;
import org.knime.core.data.sort.RowComparator;
import org.knime.core.data.sort.RowComparator.SortKeyColumns;
import org.knime.core.data.v2.RowRead;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.RandomRowAccessible;

/**
 *
 * @author leonard.woerteler
 */
public final class ColumnarTableSorter {

    /**
     * @return a {@link LookaheadRowAccessible} for the given columnarTable
     * @throws IllegalArgumentException if the table is not columnar
     */
    private static RandomRowAccessible getRowAccessible(final ColumnarTableBackend backend,
        final BufferedDataTable columnarTable, final ExecutionContext exec) {
        var delegate = ColumnarTableBackend.unwrap(columnarTable);
        if (delegate instanceof AbstractColumnarContainerTable containerTable) {
            return containerTable.asRowAccessible();
        } else if (delegate instanceof VirtualTableExtensionTable) {
            // TODO we do not always need to re-write
            // we could get a cursor that computes the virtual table output on demand
            return getRowAccessible(backend, ColumnarTableBackend.rewriteTable(columnarTable, exec), exec);
        } else {
            throw new IllegalArgumentException("the table is not columnar");
        }
    }

    public static BufferedDataTable sort(final ColumnarTableBackend backend, final ExecutionContext exec,
        final ColumnarValueSchema schema, final BufferedDataTable columnarTable,
        final RowComparator rowComparator) {

        var MAX_SIZE = 10_000; // TODO num rows for now but should be bytes

        final var initialRuns = phase0(backend, columnarTable, schema, exec, rowComparator, MAX_SIZE);



        // Next:
        // - On the readTable we can create a cursor again (on access level)
        // var sortedReadCursor = sortedTable.createCursor();
        // - We can wrap the access of the cursor as RowRead:
        // var rowRead = VirtualTableUtils.createRowRead(schema, sortedReadCursor.access(), keySelection);
        // - We can do this on k tables for the merge phase and merge them (writing like above)
        // - For the last merge we might need to use a "ColumnarRowContainer".
        //   It just wraps a ColumnarRowWriteTable but finish will do the wrapping to "BufferedDataTable"

        return null;
    }

    @SuppressWarnings("resource")
    private static List<ColumnarRowReadTable> phase0(final ColumnarTableBackend backend,
        final BufferedDataTable columnarTable, final ColumnarValueSchema schema, final ExecutionContext exec,
        final RowComparator rowComparator, final int maxSize) {

        ColumnStoreFactory storeFactory = getColumnsStoreFactory();
        final var keySpec = rowComparator.getSortKeyColumns();
        final var lut = computeLookupTable(keySpec);

        final var runs = new ArrayList<ColumnarRowReadTable>();

        // NB: No selection to the key column because we use the cursor later for copying the data
        try (final var rowAccessible = getRowAccessible(backend, columnarTable, exec);
                var readCursor = rowAccessible.createCursor()) {
            var keySelection = new DefaultColumnSelection(schema.numColumns()); // TODO select only the sort key columns
            final var rowRead = VirtualTableUtils.createRowRead(schema, readCursor.access(), keySelection);

            var currRow = 0;
            boolean hasNext = readCursor.forward();
            while (hasNext) {
                final var offset = currRow;

                final var sortKeys = new ArrayList<Object[]>();
                var outputSize = 0L;
                while (outputSize < maxSize) {
                    sortKeys.add(getSortKey(rowRead, keySpec, lut));
                    hasNext = readCursor.forward();
                    currRow++;
                    outputSize += sizeOf(rowRead);
                }

                final var sortedIndices = getSortedIndices(sortKeys, rowComparator, lut, offset);

                FileHandle fileHandle = null; // TODO create a temp file or use TempFileHandle
                final var underlyingStore = storeFactory.createStore(schema, fileHandle);
                final var enhancedStore = new DefaultColumnarBatchStore.ColumnarBatchStoreBuilder(underlyingStore) //
                    //.enableDictEncoding(true) // TODO???
                    .useColumnDataCache(ColumnarPreferenceUtils.getColumnDataCache(),
                        ColumnarPreferenceUtils.getPersistExecutor()) //
                    .build();

                try (var batchWriter = enhancedStore.getWriter();) {
                    var writeBatch = batchWriter.create(sortedIndices.length);
                    for (var i = 0; i < sortedIndices.length; i++) {
                        // TODO for each column
                        // TODO set to the value which we get from the input table
                        writeBatch.get(0);
                    }
                }

                try (var writeTable = new ColumnarRowWriteTable(schema, storeFactory,
                    new ColumnarRowWriteTableSettings(true, false, 0, false, true, false, 100, 1));
                        var writeCursor = writeTable.getWriteCursor()) {

                    for (var i : sortedIndices) {
                        readCursor.moveTo(i);
                        writeCursor.forward();
                        writeCursor.access().setFrom(readCursor.access());
                    }

                    // NB: This might be incorrect. The API is quite unclear. It will change in AP-21999.
                    writeCursor.flush();

                    runs.add(writeTable.finish());
                }
                readCursor.moveTo(currRow - 1);
            }
        } catch (IOException ex) {
            // TODO Handle correctly
            throw new IllegalStateException(ex);
        }
        return runs;
    }

    /**
     * @param rowRead
     */
    private static long sizeOf(final RowRead rowRead) {
        return 1L;
    }

    private static ColumnStoreFactory getColumnsStoreFactory() {
        try {
            return ColumnStoreFactoryRegistry.getOrCreateInstance().getFactorySingleton();
        } catch (Exception ex) {
            throw ex instanceof RuntimeException rte ? rte : new IllegalStateException(ex.getMessage(), ex);
        }
    }

    /**
     * @param sortKeys
     * @return
     */
    private static int[] getSortedIndices(final List<Object[]> sortKeys, final RowComparator comp, final int[] lut,
            final int offset) {
        final Integer[] indexes = IntStream.range(0, sortKeys.size()).boxed().toArray(Integer[]::new);
        final var rowL = new DummyRow(lut);
        final var rowR = new DummyRow(lut);
        final Comparator<Integer> rowComp =
                (a, b) -> comp.compare(rowL.withSortKey(sortKeys.get(a)), rowR.withSortKey(sortKeys.get(b)));
        Arrays.sort(indexes, rowComp);
        return Arrays.stream(indexes).mapToInt(idx -> idx + offset).toArray();
    }

    private static int[] computeLookupTable(final SortKeyColumns keySpec) {
        final var columnIndexes = keySpec.columnIndexes();
        final var numColumns = columnIndexes.cardinality();
        final var columnLookup = new int[numColumns];
        Arrays.fill(columnLookup, -1);

        int colIdx = -1;
        for (var i = 0; i < numColumns; i++) {
            colIdx = columnIndexes.nextSetBit(colIdx + 1);
            columnLookup[colIdx] = 1;
        }

        return columnLookup;
    }

    private static Object[] getSortKey(final RowRead rowRead, final SortKeyColumns keySpec,
            final int[] colOffsets) {
        final var row = new Object[keySpec.comparesRowKey() ? (colOffsets.length + 1) : colOffsets.length];
        final var cols = keySpec.columnIndexes();
        for (int colIdx = cols.nextSetBit(0), out = 0; colIdx >= 0; colIdx = cols.nextSetBit(colIdx + 1), out++) {
            row[out] = rowRead.isMissing(colIdx) ? DataType.getMissingCell()
                : rowRead.getValue(colIdx).materializeDataCell();
        }
        if (keySpec.comparesRowKey()) {
            row[row.length - 1] = new RowKey(rowRead.getRowKey().getString());
        }
        return row;
    }


    private static final class DummyRow implements DataRow {

        private final int[] m_colOffsets;

        private Object[] m_sortKey;

        private DummyRow(final int[] colOffsets) {
            m_colOffsets = colOffsets;
        }

        DummyRow withSortKey(final Object[] sortKey) {
            m_sortKey = sortKey;
            return this;
        }

        @Override
        public Iterator<DataCell> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getNumCells() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowKey getKey() {
            return (RowKey)m_sortKey[m_sortKey.length - 1];
        }

        @Override
        public DataCell getCell(final int index) {
            return (DataCell)m_sortKey[m_colOffsets[index]];
        }
    }
}
