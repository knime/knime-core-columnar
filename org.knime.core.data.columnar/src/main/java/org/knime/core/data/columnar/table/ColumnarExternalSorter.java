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
 *   Apr 2, 2024 (leonard.woerteler): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.commons.lang3.Functions.FailableConsumer;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.virtual.VirtualTableUtils;
import org.knime.core.data.columnar.table.virtual.WriteAccessRowWrite;
import org.knime.core.data.sort.ExternalSorter;
import org.knime.core.data.sort.KWayMergeCursor;
import org.knime.core.data.sort.RowReadComparator;
import org.knime.core.data.sort.RowReadComparator.SortKeyColumns;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;
import org.knime.core.table.row.WriteAccessRow;

/**
 * External sorter for {@link BufferedDataTable}s.
 *
 * @author Leonard Wörteler, KNIME GmbH, Konstanz, Germany
 * @since 5.3
 */
public final class ColumnarExternalSorter extends ExternalSorter<AbstractColumnarContainerTable> {

    private static final ColumnStoreFactory FACTORY = getColumnStoreFactory();

    private final MemoryAlertSystem m_memService;

    private final ColumnarValueSchema m_outputSchema;

    private final SortKeyColumns m_keySpec;

    private final FilteredColumnSelection m_sortColumnSelection;

    private final int[] m_keyColumnIndexes;

    private final IntSupplier m_tableIDSupplier;


    /**
     * @param memService
     * @param tableIDSupplier
     * @param outputSchema
     * @param comparator
     */
    public ColumnarExternalSorter(final MemoryAlertSystem memService, final IntSupplier tableIDSupplier,
            final ColumnarValueSchema outputSchema, final RowReadComparator comparator) {
        super(comparator);
        m_memService = memService;
        m_tableIDSupplier = tableIDSupplier;
        m_outputSchema = CheckUtils.checkArgumentNotNull(outputSchema, "Output schema may not be `null`");
        final var numColumnsInclRowKey = outputSchema.numColumns();
        final var numDataColumns = numColumnsInclRowKey - 1;
        m_keySpec = comparator.getSortKeyColumns().orElse(SortKeyColumns.all(numDataColumns));
        m_keyColumnIndexes = computeLookupTable(m_keySpec, numDataColumns);
        m_sortColumnSelection = new FilteredColumnSelection(numColumnsInclRowKey,
            IntStream.concat(IntStream.of(0).filter(i -> m_keySpec.comparesRowKey()),
                m_keySpec.columnIndexes().stream().map(i -> i + 1)).toArray());
    }

    /**
     * @param memService
     * @param tableIDSupplier
     * @param outputSchema
     * @param comparator
     * @param numRunsPerMerge
     */
    public ColumnarExternalSorter(final MemoryAlertSystem memService, final IntSupplier tableIDSupplier,
            final ColumnarValueSchema outputSchema, final RowReadComparator comparator, final int numRunsPerMerge) {
        super(comparator, numRunsPerMerge);
        m_memService = memService;
        m_tableIDSupplier = tableIDSupplier;
        m_outputSchema = CheckUtils.checkArgumentNotNull(outputSchema, "Output schema may not be `null`");
        final var numColumnsInclRowKey = outputSchema.numColumns();
        final var numDataColumns = numColumnsInclRowKey - 1;
        m_keySpec = comparator.getSortKeyColumns().orElse(SortKeyColumns.all(numDataColumns));
        m_keyColumnIndexes = computeLookupTable(m_keySpec, numDataColumns);
        m_sortColumnSelection = new FilteredColumnSelection(numColumnsInclRowKey,
            IntStream.concat(IntStream.of(0).filter(i -> m_keySpec.comparesRowKey()),
                m_keySpec.columnIndexes().stream().map(i -> i + 1)).toArray());
    }

    private static ColumnStoreFactory getColumnStoreFactory() {
        try {
            return ColumnStoreFactoryRegistry.getOrCreateInstance().getFactorySingleton();
        } catch (Exception ex) {
            throw ex instanceof RuntimeException rte ? rte : new IllegalStateException(ex.getMessage(), ex);
        }
    }

    private static int[] computeLookupTable(final SortKeyColumns keySpec, final int numDataColumns) {
        final var columnLookup = new int[numDataColumns];
        Arrays.fill(columnLookup, -1);
        final var columnIndexes = keySpec.columnIndexes();
        for (int offset = 0, colIdx = columnIndexes.nextSetBit(0); colIdx >= 0;
                offset++, colIdx = columnIndexes.nextSetBit(colIdx + 1)) {
            columnLookup[colIdx] = offset;
        }
        return columnLookup;
    }

    private Object[] materializeSortKey(final RowRead rowRead) {
        final var cols = m_keySpec.columnIndexes();
        final var row = new Object[cols.cardinality() + (m_keySpec.comparesRowKey() ? 1 : 0)];
        for (int colIdx = cols.nextSetBit(0), out = 0; colIdx >= 0; colIdx = cols.nextSetBit(colIdx + 1), out++) {
            row[out] = rowRead.getAsDataCell(colIdx);
        }
        if (m_keySpec.comparesRowKey()) {
            row[row.length - 1] = new RowKey(rowRead.getRowKey().getString());
        }
        return row;
    }

    private int[] sortKeysInMemory(final List<Object[]> sortKeys) {
        final Integer[] indexes = IntStream.range(0, sortKeys.size()).boxed().toArray(Integer[]::new);
        final var rowL = new CachedSortKeyRowRead(m_keyColumnIndexes);
        final var rowR = new CachedSortKeyRowRead(m_keyColumnIndexes);
        Arrays.sort(indexes,
            (a, b) -> m_comparator.compare(rowL.withSortKey(sortKeys.get(a)), rowR.withSortKey(sortKeys.get(b))));
        return Arrays.stream(indexes).mapToInt(Integer::intValue).toArray();
    }

    @Override
    protected Optional<AbstractColumnarContainerTable[]> createInitialRuns(final ExecutionContext exec, // NOSONAR
            final AbstractColumnarContainerTable inputTable, final Progress progress, final AtomicLong rowsReadCounter)
            throws IOException, CanceledExecutionException {

        // return createInitialRunsGreedyRandomAccess(exec, inputTable, progress, rowsReadCounter);
        return createInitialRunsFixedRandomAccess(exec, inputTable, progress, rowsReadCounter, 10_000_000);
    }

    @SuppressWarnings("resource")
    private Optional<AbstractColumnarContainerTable[]> createInitialRunsFixedRandomAccess(final ExecutionContext exec,
            final AbstractColumnarContainerTable inputTable, final Progress progress, final AtomicLong rowsReadCounter,
            final int numRowsPerRun) throws CanceledExecutionException, IOException {
        final var numRows = inputTable.size();
        if (numRows < 2) {
            return Optional.empty();
        }

        final var runs = new ArrayList<AbstractColumnarContainerTable>();

        try (final var rowAccessible = inputTable.asRowAccessible();
                // NB: No selection to the key column because we use the cursor later for copying the data
                final var readCursor = rowAccessible.createCursor()) {

            final var access = readCursor.access();
            final var rowRead = VirtualTableUtils.createRowRead(inputTable.getSchema(), access, m_sortColumnSelection);

            var runStart = 0L;
            boolean hasNext = readCursor.forward();

            var rowsWritten = new AtomicLong();
            while (hasNext) {
                final var sortKeys = new ArrayList<Object[]>();
                progress.update(() -> "Filling buffer");

                while (hasNext && sortKeys.size() < numRowsPerRun) {
                    progress.update((1.0 * rowsReadCounter.incrementAndGet() + rowsWritten.get()) / (2 * numRows));
                    progress.checkCanceled();
                    sortKeys.add(materializeSortKey(rowRead));
                    hasNext = readCursor.forward();
                }

                final var sortedIndices = sortKeysInMemory(sortKeys);

                final var offset = runStart;
                final var rowsWrittenHere = new AtomicInteger();
                final var fraction = progress.newFractionBuilder(rowsWrittenHere::longValue, sortedIndices.length);
                progress.update(() -> fraction.apply( //
                    new StringBuilder("Writing temporary table (row ")).append(")").toString());

                runs.add(writeTable(exec, writeCursor -> {
                    for (var i = 0; i < sortedIndices.length; i++) {
                        progress.checkCanceled();
                        readCursor.moveTo(offset + sortedIndices[i]);
                        writeCursor.forward();
                        writeCursor.access().setFrom(access);
                        rowsWrittenHere.incrementAndGet();
                        progress.update((1.0 * rowsReadCounter.get() + rowsWritten.incrementAndGet()) / (2 * numRows));
                    }
                }));
                runStart += sortKeys.size();
                readCursor.moveTo(runStart - 1);
            }

            // transfer ownership of the initial runs back to the caller
            final var finishedRuns = runs.toArray(AbstractColumnarContainerTable[]::new);
            runs.clear();
            return Optional.of(finishedRuns);

        } finally {
            // `runs` only contains tables at this point if the method terminates with an exception, clean up
            for (final var unclosedRun : runs) {
                clearTable(exec, unclosedRun);
            }
        }
    }

    @SuppressWarnings({"unused", "resource"})
    private Optional<AbstractColumnarContainerTable[]> createInitialRunsGreedyRandomAccess(final ExecutionContext exec,
        final AbstractColumnarContainerTable inputTable, final Progress progress, final AtomicLong rowsReadCounter)
        throws CanceledExecutionException, IOException {
        final var numRows = inputTable.size();
        if (numRows < 2) {
            return Optional.empty();
        }

        final var runs = new ArrayList<AbstractColumnarContainerTable>();

        try (final var rowAccessible = inputTable.asRowAccessible();
                // NB: No selection to the key column because we use the cursor later for copying the data
                final var readCursor = rowAccessible.createCursor()) {

            final var access = readCursor.access();
            final var rowRead = VirtualTableUtils.createRowRead(inputTable.getSchema(), access, m_sortColumnSelection);

            var runStart = 0L;
            boolean hasNext = readCursor.forward();

            var rowsWritten = new AtomicLong();
            final var memObservable = m_memService.newIndicator();
            while (hasNext) {
                final var sortKeys = new ArrayList<Object[]>();
                progress.update(() -> "Filling buffer");

                while (hasNext && (sortKeys.size() < m_numRunsPerMerge || !memObservable.lowMemoryActionRequired())) {
                    progress.update((1.0 * rowsReadCounter.incrementAndGet() + rowsWritten.get()) / (2 * numRows));
                    progress.checkCanceled();
                    sortKeys.add(materializeSortKey(rowRead));
                    hasNext = readCursor.forward();
                }

                final var sortedIndices = sortKeysInMemory(sortKeys);

                final var offset = runStart;
                final var rowsWrittenHere = new AtomicInteger();
                final var fraction = progress.newFractionBuilder(rowsWrittenHere::longValue, sortedIndices.length);
                progress.update(() -> fraction.apply( //
                    new StringBuilder("Writing temporary table (row ")).append(")").toString());

                runs.add(writeTable(exec, writeCursor -> {
                    for (var i = 0; i < sortedIndices.length; i++) {
                        progress.checkCanceled();
                        readCursor.moveTo(offset + sortedIndices[i]);
                        writeCursor.forward();
                        writeCursor.access().setFrom(access);
                        rowsWrittenHere.incrementAndGet();
                        progress.update((1.0 * rowsReadCounter.get() + rowsWritten.incrementAndGet()) / (2 * numRows));
                    }
                }));
                runStart += sortKeys.size();
                readCursor.moveTo(runStart - 1);
            }

            // transfer ownership of the initial runs back to the caller
            final var finishedRuns = runs.toArray(AbstractColumnarContainerTable[]::new);
            runs.clear();
            return Optional.of(finishedRuns);

        } finally {
            // `runs` only contains tables at this point if the method terminates with an exception, clean up
            for (final var unclosedRun : runs) {
                clearTable(exec, unclosedRun);
            }
        }
    }

    @Override
    protected AbstractColumnarContainerTable[] createInitialRuns(final ExecutionContext exec, final RowCursor input,
            final long optNumRows, final Progress progress, final AtomicLong rowsReadCounter)
            throws IOException, CanceledExecutionException {
        // return createInitialRunsBuffering(exec, input, progress, rowsReadCounter); // NOSONAR
        return createInitialRunsGreedy(exec, input, optNumRows, progress, rowsReadCounter);
    }

    private AbstractColumnarContainerTable[] createInitialRunsGreedy(final ExecutionContext exec, final RowCursor input,
        final long optNumRows, final Progress progress, final AtomicLong rowsReadCounter)
        throws CanceledExecutionException, IOException {
        final var lowMemIndicator = m_memService.newIndicator();
        return createInitialRunsGreedy(exec, lowMemIndicator::lowMemoryActionRequired, m_numRunsPerMerge, input,
            optNumRows, progress, rowsReadCounter).toArray(AbstractColumnarContainerTable[]::new);
    }

    @Override
    protected AbstractColumnarContainerTable writeRun(final ExecutionContext exec, final Progress progress,
            final List<DataRow> buffer) throws CanceledExecutionException, IOException {
        final var numRowsWritten = new AtomicInteger();
        final int totalBufferSize = buffer.size();

        // instantiate the supplier only once
        final var fraction = progress.newFractionBuilder(numRowsWritten::longValue, totalBufferSize);
        Supplier<String> messageSupplier =
            () -> fraction.apply(new StringBuilder("Writing temporary table (row ")).append(")").toString();

        return writeTable(exec, output -> { // NOSONAR not too long
            final var rowWrite = new WriteAccessRowWrite(m_outputSchema, output.access());
            for (var i = 0; i < totalBufferSize; i++) {
                progress.checkCanceled();

                // notify the progress monitor that something has changed
                progress.update(messageSupplier);

                // must not use Iterator#remove as it causes array copies
                output.forward();
                rowWrite.setFrom(buffer.set(i, null));
                numRowsWritten.incrementAndGet();
            }

            // reset buffer (contains only `null`s now)
            buffer.clear();
        });
    }

    @SuppressWarnings({"resource", "unused"})
    private AbstractColumnarContainerTable[] createInitialRunsBuffering(final ExecutionContext exec,
            final RowCursor input, final Progress progress, final AtomicLong rowsReadCounter)
            throws IOException, CanceledExecutionException {
        final var runs = new ArrayList<AbstractColumnarContainerTable>();
        try {
            while (input.canForward()) {
                final var sortKeys = new ArrayList<Object[]>();
                final Supplier<String> readMsgSupplier = () -> "Filling buffer";

                final var bufferTable = writeTable(exec, output -> { // NOSONAR
                    final var rowWrite = new WriteAccessRowWrite(m_outputSchema, output.access());
                    do {
                        progress.checkCanceled();
                        output.forward();
                        final var rowRead = input.forward();
                        rowWrite.setFrom(rowRead);
                        rowsReadCounter.incrementAndGet();
                        sortKeys.add(materializeSortKey(rowRead));
                        progress.update(readMsgSupplier);
                    } while (input.canForward() && sortKeys.size() < 1_000_000);
                });

                progress.update(() -> "Sorting in-memory buffer...");
                final var sortedIndices = sortKeysInMemory(sortKeys);

                final var rowsWrittenCounter = new AtomicInteger();
                final var fractionBuilder = progress.newFractionBuilder(rowsWrittenCounter::get, sortKeys.size());
                final Supplier<String> writeMsgSupplier = () -> fractionBuilder.apply(
                    new StringBuilder("Writing temporary table (row ")).append(")").toString();

                try (final var bufferRead = new DeletingRowCursor(bufferTable, m_sortColumnSelection)) {
                    runs.add(writeTable(exec, writeCursor -> {
                        for (var i = 0; i < sortedIndices.length; i++) { // NOSONAR
                            progress.checkCanceled();
                            rowsWrittenCounter.incrementAndGet();
                            progress.update(writeMsgSupplier);
                            bufferRead.moveTo(sortedIndices[i]);
                            writeCursor.forward();
                            writeCursor.access().setFrom(bufferRead.access());
                        }
                    }));
                }
            }

            // transfer ownership of the initial runs back to the caller
            final var finishedRuns = runs.toArray(AbstractColumnarContainerTable[]::new);
            runs.clear();
            return finishedRuns;

        } finally {
            // `runs` only contains tables at this point if the method terminates with an exception, clean up
            for (final var unclosedRun : runs) {
                clearTable(exec, unclosedRun);
            }
        }
    }

    @Override
    protected AbstractColumnarContainerTable mergeToTable(final ExecutionContext exec,
            final List<AbstractColumnarContainerTable> runsToMerge, final Progress progress,
            final AtomicLong rowsProcessedCounter, final long numOutputRows, final Runnable beforeFinishing)
            throws CanceledExecutionException, IOException {

        final var deletingCursors = runsToMerge.stream() //
                .map(run -> new DeletingRowCursor(run, m_sortColumnSelection)) //
                .toArray(DeletingRowCursor[]::new);

        final var numDataColumns = m_outputSchema.numColumns() - 1;
        try (final var mergeCursor = new KWayMergeCursor(m_comparator, deletingCursors, numDataColumns)) {
            return writeTable(exec, writeCursor -> {
                while (mergeCursor.canForward()) {
                    progress.checkCanceled();
                    mergeCursor.forward();
                    writeCursor.forward();
                    writeCursor.access().setFrom(deletingCursors[mergeCursor.currentInputIndex()].access());
                    progress.update(1.0 * rowsProcessedCounter.incrementAndGet() / numOutputRows);
                }

                // notify the progress monitor that the message has changed
                beforeFinishing.run();
            });
        }
    }


    @SuppressWarnings("resource")
    private AbstractColumnarContainerTable writeTable(final ExecutionContext exec,
            final FailableConsumer<WriteCursor<WriteAccessRow>, CanceledExecutionException> body)
            throws IOException, CanceledExecutionException {
        final var settings = new ColumnarRowWriteTableSettings(
            true, // initializeDomains
            false, // calculateDomains
            0, // maxPossibleNominalDomainValues
            false, // checkDuplicateRowKeys
            true, // useCaching
            false, // forceSynchronousIO
            100, // rowBatchSize
            1);

        try (final var container =
                    ColumnarRowContainer.create(exec, m_tableIDSupplier.getAsInt(), m_outputSchema, FACTORY, settings);
                final var writeCursor = container.createCursor()) {
            body.accept(writeCursor.getAccessCursor());
            return (AbstractColumnarContainerTable)container.finishInternal();
        }
    }

    @Override
    protected RowCursor createMergedCursor(final ExecutionContext exec,
            final List<AbstractColumnarContainerTable> runsToMerge) {
        final int numDataColumns = m_outputSchema.numColumns() - 1;
        final var selectAll = ColumnSelection.fromSelection(Selection.all(), numDataColumns);

        final var cursors = runsToMerge.stream() //
                .map(run -> new DeletingRowCursor(run, selectAll)) //
                .toArray(RowCursor[]::new);
        return cursors.length == 1 ? cursors[0] : new KWayMergeCursor(m_comparator, cursors, numDataColumns);
    }

    @SuppressWarnings("resource")
    @Override
    protected void clearTable(final ExecutionContext ctx, final AbstractColumnarContainerTable table) {
        try {
            table.close();
        } finally {
            table.getStore().getFileHandle().delete();
        }
    }

    private static final class DeletingRowCursor implements RowCursor {

        private AbstractColumnarContainerTable m_run;
        private RandomAccessCursor<ReadAccessRow> m_delegate;
        private RowRead m_rowRead;


        @SuppressWarnings("resource")
        DeletingRowCursor(final AbstractColumnarContainerTable run, final ColumnSelection keyColumnFilter) {
            m_run = run;
            m_delegate = run.asRowAccessible().createCursor();
            m_rowRead = VirtualTableUtils.createRowRead(run.getSchema(), m_delegate.access(), keyColumnFilter);
        }

        void moveTo(final long rowIndex) {
            m_delegate.moveTo(rowIndex);
        }

        ReadAccessRow access() {
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
                    NodeLogger.getLogger(ColumnarExternalSorter.class).error(ex);
                } finally {
                    m_run.getStore().getFileHandle().delete();
                    m_run = null;
                }
            }
        }
    }

    private static final class CachedSortKeyRowRead implements RowRead {

        private final int[] m_colOffsets;

        private Object[] m_sortKey;

        CachedSortKeyRowRead(final int[] colOffsets) {
            m_colOffsets = colOffsets;
        }

        RowRead withSortKey(final Object[] sortKey) {
            m_sortKey = sortKey;
            return this;
        }

        @Override
        public DataCell getAsDataCell(final int index) {
            return (DataCell)m_sortKey[m_colOffsets[index]];
        }

        @Override
        public int getNumColumns() {
            return m_colOffsets.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <D extends DataValue> D getValue(final int index) {
            return (D)getAsDataCell(index);
        }

        @Override
        public boolean isMissing(final int index) {
            return getAsDataCell(index).isMissing();
        }

        @Override
        public RowKeyValue getRowKey() {
            return (RowKeyValue)m_sortKey[m_sortKey.length - 1];
        }
    }
}
