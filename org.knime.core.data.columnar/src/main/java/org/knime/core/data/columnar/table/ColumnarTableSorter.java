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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.ColumnarTableBackend;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.virtual.VirtualTableUtils;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.sort.KWayMergeCursor;
import org.knime.core.data.sort.RowComparator;
import org.knime.core.data.sort.RowReadComparator;
import org.knime.core.data.sort.RowReadComparator.SortKeyColumns;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;

/**
 *
 * @author leonard.woerteler
 */
public final class ColumnarTableSorter {
    private static final int MAX_SIZE = 1_000_000; // TODO num rows for now but should be bytes
    private static final int K = 10;

    private final ExecutionContext m_execContext;
    private final BufferedDataTable m_inputTable;
    private final ColumnarValueSchema m_schema;
    private final ColumnStoreFactory m_columnStoreFactory;

    public ColumnarTableSorter(final ExecutionContext exec, final ColumnarValueSchema schema,
            final BufferedDataTable inputTable) {
        m_execContext = exec;
        m_inputTable = inputTable;
        m_schema = schema;
        try {
            m_columnStoreFactory = ColumnStoreFactoryRegistry.getOrCreateInstance().getFactorySingleton();
        } catch (Exception ex) {
            throw ex instanceof RuntimeException rte ? rte : new IllegalStateException(ex.getMessage(), ex);
        }
    }

    public KnowsRowCountTable sortIntoTable(final IntSupplier tableIDSupplier, final RowComparator rowComparator)
            throws IOException {
        final var keySpec = rowComparator.getSortKeyColumns();
        final var columnIndexLookup = computeLookupTable(keySpec);
        final var rowReadComparator = rowComparator.toRowReadComparator();
        final var filteredKeySelection = createSortColumnSelection(keySpec);

        final var initialRuns = phase0(keySpec, filteredKeySelection, columnIndexLookup, rowReadComparator, MAX_SIZE);
        return mergePhase(tableIDSupplier, initialRuns, filteredKeySelection, rowReadComparator, K);
    }

    public RowCursor sortIntoCursor(final RowComparator rowComparator) throws IOException {
        final var keySpec = rowComparator.getSortKeyColumns();
        final var columnIndexLookup = computeLookupTable(keySpec);
        final var rowReadComparator = rowComparator.toRowReadComparator();
        final var filteredKeySelection = createSortColumnSelection(keySpec);

        final var initialRuns = phase0(keySpec, filteredKeySelection, columnIndexLookup, rowReadComparator, MAX_SIZE);
        return mergePhaseToCursor(initialRuns, filteredKeySelection, rowReadComparator, K);
    }

    @SuppressWarnings("resource")
    public CloseableRowIterator sortIntoIterator(final RowComparator rowComparator) throws IOException {
        final var sortedCursor = sortIntoCursor(rowComparator);
        return new CursorToIteratorAdapter(sortedCursor);
    }

    @SuppressWarnings("resource")
    private List<ColumnarRowReadTable> phase0(final SortKeyColumns keySpec, final FilteredColumnSelection keyFilter,
            final int[] columnIndexLookup, final RowReadComparator rowReadComparator, final int maxSize)
            throws IOException {

        final var runs = new ArrayList<ColumnarRowReadTable>();

        // NB: No selection to the key column because we use the cursor later for copying the data
        try (final var rowAccessible = getRowAccessible(m_inputTable);
                final var readCursor = rowAccessible.createCursor()) {
            final var rowRead = VirtualTableUtils.createRowRead(m_schema, readCursor.access(), keyFilter);

            var runStart = 0L;
            boolean hasNext = readCursor.forward();
            while (hasNext) {

                final var sortKeys = new ArrayList<Object[]>();
                var outputSize = 0L;
                while (hasNext && outputSize < maxSize) {
                    sortKeys.add(getSortKey(rowRead, keySpec, columnIndexLookup));
                    hasNext = readCursor.forward();
                    outputSize += sizeOf(readCursor);
                }

                final var sortedIndices = getSortedIndices(sortKeys, rowReadComparator, columnIndexLookup);

                try (var writeTable = new ColumnarRowWriteTable(m_schema, m_columnStoreFactory,
                    new ColumnarRowWriteTableSettings(true, false, 0, false, true, false, 100, 1));
                        var writeCursor = writeTable.getWriteCursor()) {

                    final var outAccess = writeCursor.access();
                    final var inAccess = readCursor.access();
                    for (var i : sortedIndices) {
                        readCursor.moveTo(runStart + i);
                        writeCursor.forward();
                        outAccess.setFrom(inAccess);
                    }

                    runs.add(writeTable.finish());
                }
                runStart += sortKeys.size();
                readCursor.moveTo(runStart - 1);
            }
        }
        return runs;
    }

    @SuppressWarnings("resource")
    private KnowsRowCountTable mergePhase(final IntSupplier tableIDSupplier,
            final List<ColumnarRowReadTable> initialRuns, final FilteredColumnSelection keyColumnFilter,
            final RowReadComparator rowReadComparator, final int maxK) throws IOException {
        final var mergeQueue = new ArrayDeque<>(initialRuns);
        initialRuns.clear();

        while (mergeQueue.size() > 1) {
            mergeIteration(mergeQueue, keyColumnFilter, rowReadComparator, maxK);
        }
        return UnsavedColumnarContainerTable.create(tableIDSupplier.getAsInt(), mergeQueue.removeFirst(), () -> {});
    }

    @SuppressWarnings("resource")
    private RowCursor mergePhaseToCursor(final List<ColumnarRowReadTable> initialRuns,
            final FilteredColumnSelection keyColumnFilter, final Comparator<RowRead> rowReadComparator, final int k)
            throws IOException {
        final var mergeQueue = new ArrayDeque<>(initialRuns);
        initialRuns.clear();

        while (mergeQueue.size() > k) {
            mergeIteration(mergeQueue, keyColumnFilter, rowReadComparator, k);
        }

        if (mergeQueue.size() == 1) {
            final var resultTable = initialRuns.get(0);
            return new DeletingRowCursor(resultTable, new DefaultColumnSelection(m_schema.numColumns()));
        }

        final var rowCursors = mergeQueue.stream() //
                .map(run -> new DeletingRowCursor(run, keyColumnFilter)) //
                .toArray(RowCursor[]::new);
        return new KWayMergeCursor(rowReadComparator, rowCursors, m_schema.numColumns());
    }

    @SuppressWarnings("resource")
    private void mergeIteration(final Deque<ColumnarRowReadTable> runs, final FilteredColumnSelection keyFilter,
            final Comparator<RowRead> rowReadComparator, final int maxK) throws IOException {
        final var numInputRuns = runs.size();
        final var numMerges = (numInputRuns + maxK - 1) / maxK;
        final var minNumInputs = numInputRuns / numMerges;
        final var numAdditionalRuns = numInputRuns % numMerges;

        final int len = numAdditionalRuns != 0 ? (minNumInputs + 1) : minNumInputs;
        final var rowCursors = new DeletingRowCursor[len];

        for (var i = 0; i < numMerges; i++) {
            final var k = i < numAdditionalRuns ? (minNumInputs + 1) : minNumInputs;
            for (var j = 0; j < k; j++) {
                final ColumnarRowReadTable run = runs.removeFirst();
                rowCursors[j] = new DeletingRowCursor(run, keyFilter);
            }
            runs.add(merge(rowReadComparator, rowCursors));
            Arrays.fill(rowCursors, null);
        }
    }

    private ColumnarRowReadTable merge(final Comparator<RowRead> comparator, final DeletingRowCursor[] inputs)
            throws IOException {
        try (final var mergeCursor = new KWayMergeCursor(comparator, inputs, m_schema.numColumns());
                var writeTable = new ColumnarRowWriteTable(m_schema, m_columnStoreFactory,
                    new ColumnarRowWriteTableSettings(true, false, 0, false, true, false, 100, 1));
                        var writeCursor = writeTable.getWriteCursor()) {
            while (mergeCursor.canForward()) {
                mergeCursor.forward();
                writeCursor.forward();
                writeCursor.access().setFrom(inputs[mergeCursor.currentInput()].access());
            }

            // NB: This might be incorrect. The API is quite unclear. It will change in AP-21999.
            writeCursor.flush();

            return writeTable.finish();
        }
    }

    /**
     * @return a {@link LookaheadRowAccessible} for the given columnarTable
     * @throws IllegalArgumentException if the table is not columnar
     */
    private RandomRowAccessible getRowAccessible(final BufferedDataTable table) {
        var delegate = ColumnarTableBackend.unwrap(table);
        if (delegate instanceof AbstractColumnarContainerTable containerTable) {
            return containerTable.asRowAccessible();
        } else if (delegate instanceof VirtualTableExtensionTable) {
            // TODO we do not always need to re-write
            // we could get a cursor that computes the virtual table output on demand
            return getRowAccessible(ColumnarTableBackend.rewriteTable(table, m_execContext));
        } else {
            throw new IllegalArgumentException("the table is not columnar");
        }
    }

    private static long sizeOf(@SuppressWarnings("unused") final RandomAccessCursor<ReadAccessRow> cursor) {
        return 1L; // NOSONAR
    }

    private FilteredColumnSelection createSortColumnSelection(final SortKeyColumns keySpec) {
        return new FilteredColumnSelection(m_schema.numColumns(),
            IntStream.concat(IntStream.of(0).filter(i -> keySpec.comparesRowKey()),
                keySpec.columnIndexes().stream().map(i -> i + 1)).toArray());
    }

    private static int[] getSortedIndices(final List<Object[]> sortKeys, final Comparator<RowRead> comp,
            final int[] lut) {
        final Integer[] indexes = IntStream.range(0, sortKeys.size()).boxed().toArray(Integer[]::new);
        final var rowL = new DummyRowRead(lut);
        final var rowR = new DummyRowRead(lut);
        final Comparator<Integer> rowComp =
                (a, b) -> comp.compare(rowL.withSortKey(sortKeys.get(a)), rowR.withSortKey(sortKeys.get(b)));
        Arrays.sort(indexes, rowComp);
        return Arrays.stream(indexes).mapToInt(Integer::intValue).toArray();
    }

    private int[] computeLookupTable(final SortKeyColumns keySpec) {
        final var columnIndexes = keySpec.columnIndexes();
        final var columnLookup = new int[m_schema.numColumns()];
        Arrays.fill(columnLookup, -1);

        for (int offset = 0, colIdx = columnIndexes.nextSetBit(0); colIdx >= 0;
                offset++, colIdx = columnIndexes.nextSetBit(colIdx + 1)) {
            columnLookup[colIdx] = offset;
        }

        return columnLookup;
    }

    private static Object[] getSortKey(final RowRead rowRead, final SortKeyColumns keySpec,
            final int[] colOffsets) {
        final var row = new Object[keySpec.comparesRowKey() ? (colOffsets.length + 1) : colOffsets.length];
        final var cols = keySpec.columnIndexes();
        for (int colIdx = cols.nextSetBit(0), out = 0; colIdx >= 0; colIdx = cols.nextSetBit(colIdx + 1), out++) {
            row[out] = getAsDataCell(rowRead, colIdx);
        }
        if (keySpec.comparesRowKey()) {
            row[row.length - 1] = new RowKey(rowRead.getRowKey().getString());
        }
        return row;
    }

    private static DataCell getAsDataCell(final RowRead rowRead, final int colIdx) {
        return rowRead.isMissing(colIdx) ? DataType.getMissingCell() : rowRead.getValue(colIdx).materializeDataCell();
    }

    private static final class CursorToIteratorAdapter extends CloseableRowIterator {

        private final RowCursor m_cursor;

        CursorToIteratorAdapter(final RowCursor cursor) {
            m_cursor = cursor;
        }

        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final var read = m_cursor.forward();
            final var cells = IntStream.range(0, read.getNumColumns()) //
                .mapToObj(idx -> ColumnarTableSorter.getAsDataCell(read, idx)) //
                .toArray(DataCell[]::new);
            return new DefaultRow(read.getRowKey().getString(), cells);
        }

        @Override
        public boolean hasNext() {
            return m_cursor.canForward();
        }

        @Override
        public void close() {
            m_cursor.close();
        }
    }

    private static final class DeletingRowCursor implements RowCursor {

        private ColumnarRowReadTable m_run;
        private RandomAccessCursor<ReadAccessRow> m_delegate;
        private RowRead m_rowRead;


        DeletingRowCursor(final ColumnarRowReadTable run, final ColumnSelection keyColumnFilter) {
            m_run = run;
            m_delegate = run.createCursor();
            m_rowRead = VirtualTableUtils.createRowRead(run.getSchema(), m_delegate.access(), keyColumnFilter);
        }

        public ReadAccessRow access() {
            return m_delegate.access();
        }

        @Override
        public RowRead forward() {
            return m_delegate.forward() ? m_rowRead : null;
        }

        @Override
        public boolean canForward() {
            return m_delegate.canForward();
        }

        @Override
        public int getNumColumns() {
            return m_rowRead.getNumColumns();
        }

        @SuppressWarnings("resource")
        @Override
        public void close() {
            if (m_run != null) {
                try {
                    m_rowRead = null;
                    m_delegate.close();
                    m_delegate = null;
                    m_run.close();
                } catch (IOException ex) {
                    NodeLogger.getLogger(ColumnarTableSorter.class).error(ex);
                } finally {
                    m_run.getStore().getFileHandle().delete();
                    m_run = null;
                }
            }
        }
    }

    private static final class DummyRowRead implements RowRead {

        private final int[] m_colOffsets;

        private Object[] m_sortKey;

        DummyRowRead(final int[] colOffsets) {
            m_colOffsets = colOffsets;
        }

        RowRead withSortKey(final Object[] sortKey) {
            m_sortKey = sortKey;
            return this;
        }

        private DataCell getCell(final int index) {
            return (DataCell)m_sortKey[m_colOffsets[index]];
        }

        @Override
        public int getNumColumns() {
            return m_colOffsets.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <D extends DataValue> D getValue(final int index) {
            return (D)getCell(index);
        }

        @Override
        public boolean isMissing(final int index) {
            return getCell(index).isMissing();
        }

        @Override
        public RowKeyValue getRowKey() {
            return (RowKeyValue)m_sortKey[m_sortKey.length - 1];
        }
    }
}
