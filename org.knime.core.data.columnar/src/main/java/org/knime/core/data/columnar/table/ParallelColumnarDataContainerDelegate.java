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
 *   Oct 13, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.knime.core.columnar.ColumnarParameters.BATCH_SIZE_TARGET;
import static org.knime.core.columnar.ColumnarParameters.CAPACITY_MAX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.knime.core.columnar.ColumnarParameters;
import org.knime.core.columnar.parallel.exec.ColumnTask;
import org.knime.core.columnar.parallel.exec.RowTaskBatch;
import org.knime.core.columnar.parallel.exec.WriteTaskExecutor;
import org.knime.core.columnar.parallel.write.AsyncBatchWriter;
import org.knime.core.columnar.parallel.write.AsyncBatchWriter.AdjustingCapacityStrategy;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.v2.RowKeyWriteValue;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.table.access.WriteAccess;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ParallelColumnarDataContainerDelegate implements DataContainerDelegate {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4, new ThreadFactory() {
        private final AtomicInteger m_threadNum = new AtomicInteger(0);

        @Override
        public Thread newThread(final Runnable r) {
            return new Thread(r, "KNIME-Columnar-Data-Container-Executor-" + m_threadNum.getAndIncrement());
        }
    });

    private final BatchStore m_store;

    private final ColumnarRowWriteTable m_writeTable;

    private final AsyncBatchWriter m_batchWriter;

    private final WriteTaskExecutor<WriteAccess> m_writeExec;

    private final ColumnarValueSchema m_schema;

    private final DataRowBatchTaskDispatcher m_dispatcher;

    private boolean m_closed;

    private ContainerTable m_containerTable;

    private final int m_id;

    private boolean m_cleared;

    private long m_size = 0;

    // TODO implement sequential I/O for filestore columns
    // or get rid of the sequential writing in loops by introducing a flush functionality in core that automatically
    // flushes the containers after each loop iteration

    ParallelColumnarDataContainerDelegate(final int tableId, final ColumnarValueSchema schema,
        final ColumnarRowWriteTable writeTable) {
        m_writeTable = writeTable;
        m_schema = schema;
        m_store = writeTable.getStore();
        m_batchWriter = new AsyncBatchWriter(m_store,
            new AdjustingCapacityStrategy(ColumnarParameters.CAPACITY_INIT, BATCH_SIZE_TARGET, CAPACITY_MAX));
        m_writeExec = new WriteTaskExecutor<>(m_batchWriter, EXECUTOR, RowBatchWriteTask.NULL);
        m_dispatcher = new DataRowBatchTaskDispatcher(100, this::scheduleBatch);
        m_id = tableId;
    }

    @Override
    public void addRowToTable(final DataRow row) {
        m_dispatcher.addRow(row);
        m_size++;
    }

    @SuppressWarnings("resource") // the task is closed by the executor once it is finished
    private void scheduleBatch(final DataRow[] rows) {
        m_writeExec.accept(new RowBatchWriteTask(m_schema, rows));
    }

    private static final class DataRowBatchTaskDispatcher {
        private final int m_batchSize;

        private final List<DataRow> m_batch;

        private final Consumer<DataRow[]> m_batchConsumer;

        DataRowBatchTaskDispatcher(final int batchSize, final Consumer<DataRow[]> batchConsumer) {
            m_batchSize = batchSize;
            m_batch = new ArrayList<>(batchSize);
            m_batchConsumer = batchConsumer;
        }

        void addRow(final DataRow row) {
            m_batch.add(row);
            if (m_batch.size() == m_batchSize) {
                m_batchConsumer.accept(m_batch.toArray(DataRow[]::new));
                m_batch.clear();
            }
        }

        void dispatchLastBatch() {
            if (!m_batch.isEmpty()) {
                m_batchConsumer.accept(m_batch.toArray(DataRow[]::new));
            }
        }
    }

    @Override
    public void close() {
        if (!m_closed) {
            m_closed = true;
            m_dispatcher.dispatchLastBatch();

            try {
                m_writeExec.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for the table writing to finish.", ex);
            }
            @SuppressWarnings("resource") // is managed by m_containerTable
            final ColumnarRowReadTable finishedColumnarTable = m_writeTable.finish(m_size);
            closeAsyncResources();
            m_containerTable =
                UnsavedColumnarContainerTable.create(m_id, finishedColumnarTable, m_writeTable.getStoreFlusher());
        }
    }

    private void closeAsyncResources() {
        m_writeExec.close();
        try {
            m_batchWriter.close();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to close the batchWriter.", ex);
        }
    }

    @Override
    public ContainerTable getTable() {
        if (!m_closed) {
            throw new IllegalStateException("getTable() can only be called after close() was called.");
        }
        return m_containerTable;
    }

    @Override
    public void clear() {
        if (!m_cleared) {
            m_cleared = true;
            if (m_closed) {
                if (m_containerTable != null) {
                    // can be null if close failed exceptionally e.g. because of duplicate row keys
                    m_containerTable.clear();
                }
            } else {
                m_closed = true;
                closeAsyncResources();
                m_writeTable.close();
            }
        }
    }

    @Override
    public long size() {
        return m_size;
    }

    @Override
    public DataTableSpec getTableSpec() {
        return m_schema.getSourceSpec();
    }

    @Deprecated
    @Override
    public void setMaxPossibleValues(final int maxPossibleValues) {
        m_writeTable.setMaxPossibleValues(maxPossibleValues);
    }

    private static final class RowBatchWriteTask implements RowTaskBatch<WriteAccess> {

        static final RowBatchWriteTask NULL = new RowBatchWriteTask(null, null);

        private final DataRow[] m_rows;

        private final ColumnarValueSchema m_schema;

        RowBatchWriteTask(final ColumnarValueSchema schema, final DataRow[] rows) {
            m_schema = schema;
            m_rows = rows;
        }

        @Override
        public ColumnTask createColumnTask(final int colIdx, final WriteAccess writeAccess) {
            var rowConsumer = getRowConsumer(colIdx, writeAccess);
            return new RowBatchColumnWriteTask(rowConsumer);
        }

        private Consumer<DataRow> getRowConsumer(final int colIdx, final WriteAccess writeAccess) {
            final var valueFactory = m_schema.getValueFactory(colIdx);
            final var writeValue = valueFactory.createWriteValue(writeAccess);
            if (colIdx == 0) {
                return new KeyWrite((RowKeyWriteValue)writeValue);
            } else {
                return new ColumnWrite(writeValue, writeAccess, colIdx - 1);
            }
        }

        @Override
        public void close() {
            // nothing to close, all data is on heap
        }

        private static final class ColumnWrite implements Consumer<DataRow> {

            private final WriteValue<?> m_writeValue;

            private final WriteAccess m_writeAccess;

            private final int m_colIdx;

            ColumnWrite(final WriteValue<?> writeValue, final WriteAccess writeAccess, final int colIdx) {
                m_writeValue = writeValue;
                m_writeAccess = writeAccess;
                m_colIdx = colIdx;
            }

            @Override
            public void accept(final DataRow row) {
                var cell = row.getCell(m_colIdx);
                if (cell.isMissing()) {
                    m_writeAccess.setMissing();
                } else {
                    setValue(m_writeValue, cell);
                }
            }

        }

        private static final class KeyWrite implements Consumer<DataRow> {

            private final RowKeyWriteValue m_writeValue;

            KeyWrite(final RowKeyWriteValue writeValue) {
                m_writeValue = writeValue;
            }

            @Override
            public void accept(final DataRow t) {
                m_writeValue.setRowKey(t.getKey());
            }
        }

        private class RowBatchColumnWriteTask implements ColumnTask {

            private final Consumer<DataRow> m_rowConsumer;

            RowBatchColumnWriteTask(final Consumer<DataRow> rowConsumer) {
                m_rowConsumer = rowConsumer;
            }

            @Override
            public int size() {
                return m_rows.length;
            }

            @Override
            public void performSubtask(final int subtask) {
                m_rowConsumer.accept(m_rows[subtask]);
            }

        }

        @SuppressWarnings("unchecked")
        private static <D extends DataValue> void setValue(final WriteValue<D> writeValue, final DataValue value) {
            writeValue.setValue((D)value);
        }

    }

}
