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
 */
package org.knime.core.data.columnar.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.container.DataContainerException;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.DuplicateKeyException;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Tobias Pietzsch
 */
final class ColumnarDataContainerDelegate implements DataContainerDelegate {

    /** The executor, which runs the IO tasks. Currently used only while writing rows. */
    static final ThreadPoolExecutor ASYNC_EXECUTORS;

    private static final int MAX_NUM_THREADS;

    static {
        final DataContainerSettings defaults = DataContainerSettings.getDefault();
        ASYNC_EXECUTORS = new ThreadPoolExecutor(defaults.getMaxContainerThreads(), defaults.getMaxContainerThreads(),
            10L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactory() {
                private final AtomicLong m_threadCount = new AtomicLong();

                @Override
                public Thread newThread(final Runnable r) {
                    return new Thread(r, "KNIME-Columnar-Container-Thread-" + m_threadCount.getAndIncrement());
                }
            });
        ASYNC_EXECUTORS.allowCoreThreadTimeOut(true);
        MAX_NUM_THREADS = ASYNC_EXECUTORS.getMaximumPoolSize();
    }

    private final RowWriteCursor m_delegateCursor;

    private final DataTableSpec m_spec;

    private final int m_numColumns;

    private final ColumnarRowContainer m_delegateContainer;

    /** Flag indicating the memory state. */
    private boolean m_memoryLowState;

    private boolean m_closed = false;

    private boolean m_cleared = false;

    /**
     * {@link ContainerRunnable}s abort when they see that this flag is {@code true}.
     * <p>
     * The flag is set in {@link #clear()}, followed by waiting for all runnables to complete
     * ({@link #waitForAndHandleFuture()}, and finally clearing the delegate.
     */
    private AtomicBoolean m_async_abort = new AtomicBoolean(false);

    /**
     * Represents the completion of all pending {@link ContainerRunnable}s. New {@link ContainerRunnable}s are appended
     * using {@link #submit()}.
     */
    private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

    /**
     * Whether this container handles rows synchronously (as opposed to collecting and submitting batches).
     */
    private final boolean m_forceSynchronousIO;

    /** The size of each batch submitted to the {@link #ASYNC_EXECUTORS} service. */
    private final int m_batchSize;

    /**
     * This list is filled with {@code DataRow}s for asynchronous writing. When {@code m_batchSize} rows are accumulated
     * it is {@code #submit() submitted} and replaced by a new empty list.
     */
    private List<DataRow> m_curBatch;

    /**
     * Semaphore used to block the producer in case that we have more than {@link #MAX_NUM_THREADS} batches waiting to
     * be written to the {@m_delegateContainer}.
     */
    private final Semaphore m_numPendingBatches;

    /**
     * How many rows have been written to this {@code ColumnarDataContainerDelegate}.
     */
    private long m_size;

    private ContainerTable m_containerTable;

    ColumnarDataContainerDelegate(final DataTableSpec spec, final ColumnarRowContainer container, final ColumnarRowWriteTableSettings settings) {
        m_delegateContainer = container;
        m_delegateCursor = container.createCursor();
        m_spec = spec;
        m_numColumns = m_spec.getNumColumns();
        m_forceSynchronousIO = settings.isForceSynchronousIO();
        m_batchSize = settings.getRowBatchSize();
        m_curBatch = new ArrayList<>(m_batchSize);
        m_numPendingBatches = new Semaphore(settings.getMaxPendingBatches());

    }

    @Override
    public DataTableSpec getTableSpec() {
        return m_containerTable != null ? m_containerTable.getDataTableSpec() : m_spec;
    }

    @Override
    public long size() {
        return m_size;
    }

    @Deprecated
    @Override
    public void setMaxPossibleValues(final int maxPossibleValues) { // NOSONAR
        m_delegateContainer.setMaxPossibleValues(maxPossibleValues);
    }

    /**
     * Check and rethrow exceptions that occurred during asynchronous writing
     */
    private void checkAsyncWriteThrowable() {
        if (m_future.isCompletedExceptionally()) {
            waitForAndHandleFuture();
        }
    }

    /**
     * Wait for {@code m_future} to complete (normally or exceptionally).
     * Rethrow exceptions.
     */
    private void waitForAndHandleFuture() {
        try {
            m_future.get();
        } catch (ExecutionException e) {
            throw wrap(e.getCause());
        } catch (CancellationException | InterruptedException e) {
            throw wrap(e);
        }
    }

    private static RuntimeException wrap(final Throwable t) {
        final StringBuilder error = new StringBuilder();
        if (t.getMessage() != null) {
            error.append(t.getMessage());
        } else {
            error.append("Writing to table process threw \"");
            error.append(t.getClass().getSimpleName()).append("\"");
        }
        if (t instanceof DuplicateKeyException) {
            // self-causation not allowed
            return new DuplicateKeyException((DuplicateKeyException)t);
        } else {
            return new DataContainerException(error.toString(), t);
        }
    }

    /**
     * Adds the row to the table in a asynchronous/synchronous manner depending on the current memory state, see
     * {@link #m_memoryLowState}. Whenever we change into a low memory state we flush everything to disk and wait for
     * all {@link ContainerRunnable} to finish their execution, while blocking the data producer.
     *
     * @param row the row to be asynchronously processed
     */
    @Override
    public void addRowToTable(final DataRow row) {
        Objects.requireNonNull(row);
        if (m_closed) {
            throw new IllegalStateException("Container delegate has already been closed.");
        }

        if (row.getNumCells() != m_numColumns) {
            throw new IllegalArgumentException(
                String.format("Cell count in row \"%s\" is not equal to length of column names array: %d vs. %d",
                    row.getKey().toString(), row.getNumCells(), m_spec.getNumColumns()));
        }
        /* As per contract, this method should also throw an unchecked DuplicateKeyException if a row's key has already
         * been added. Here, this is done asynchronously and taken care of by the delegate ColumnarRowContainer and its
         * underlying DomainColumnStore. */

        if (MemoryAlertSystem.getInstanceUncollected().isMemoryLow()) {
            // When a low memory state occurs in consecutive addRowToTable calls, only the first time we need to flush
            if (!m_memoryLowState) {
                m_memoryLowState = true;
                submit();
                waitForAndHandleFuture();
            }
        } else {
            m_memoryLowState = false;
        }

        if (m_forceSynchronousIO || m_memoryLowState) {
            writeRowIntoCursor(row);
        } else {
            addRowToTableAsynchronously(row);
        }

        m_size++;
    }

    /**
     * Write the given {@code row} directly to the delegate cursor.
     *
     * @param row the row to be written
     */
    private void writeRowIntoCursor(final DataRow row) {
        final RowWrite rowWrite = m_delegateCursor.forward();
        rowWrite.setRowKey(row.getKey());
        for (int i = 0; i < m_numColumns; i++) {
            final DataCell cell = row.getCell(i);
            if (cell.isMissing()) {
                rowWrite.setMissing(i);
            } else {
                rowWrite.<WriteValue<DataCell>> getWriteValue(i).setValue(cell);
            }
        }
    }

    /**
     * Add the given {@code row} to the current batch, for later asynchronous writing.
     *
     * @param row the row to be written
     */
    private void addRowToTableAsynchronously(final DataRow row) {
        checkAsyncWriteThrowable();
        m_curBatch.add(row);
        if (m_curBatch.size() == m_batchSize) {
            submit();
        }
    }

    /**
     * Submits the current batch to the {@link #ASYNC_EXECUTORS} service (unless the current batch is empty).
     *
     * @throws InterruptedException if interrupted while waiting for space in the pending-batches queue
     */
    private void submit() {
        if (m_curBatch.isEmpty()) {
            return;
        }

        // wait until we are allowed to submit a new runnable
        try {
            m_numPendingBatches.acquire();
        } catch (InterruptedException e) {
            throw wrap(e);
        }

        m_future = m_future.thenRunAsync(new ContainerRunnable(m_curBatch), ASYNC_EXECUTORS);

        // reset batch
        m_curBatch = new ArrayList<>(m_batchSize);
    }

    @Override
    public ContainerTable getTable() {
        if (!m_closed) {
            throw new IllegalStateException("getTable() can only be called after close() was called.");
        }
        return m_containerTable;
    }

    @Override
    public void close() {
        if (!m_closed) {
            m_closed = true;
            submit(); // submit the last batch
            waitForAndHandleFuture();
            m_containerTable = m_delegateContainer.finishInternal();
            m_delegateCursor.close();
        }
    }

    @Override
    public void clear() {
        if (!m_cleared) {
            m_cleared = true;
            if (m_closed) {
                m_containerTable.clear();
            } else {
                m_async_abort.set(true);
                m_closed = true;
                m_curBatch = null; // discard the last batch
                waitForAndHandleFuture();
                m_delegateCursor.close();
                m_delegateContainer.close();
            }
        }
    }

    private final class ContainerRunnable implements Runnable {

        /** The batch of rows to be processed. */
        private final List<DataRow> m_rows;

        private final NodeContext m_nodeContext;

        /**
         * Constructor.
         *
         * @param domainCreator the domain creator
         * @param rows the batch of rows to be processed
         * @param batchIdx the batch index
         */
        ContainerRunnable(final List<DataRow> rows) {
            m_rows = rows;
            /**
             * The node context may be null if the DataContainer has been created outside of a node's context (e.g., in
             * unit tests). This is also the reason why this class does not extend the RunnableWithContext class.
             */
            m_nodeContext = NodeContext.getContext();
        }

        @Override
        public final void run() {
            if (m_async_abort.get()) {
                // clear() was called. Don't write anything anymore.
                return;
            }

            NodeContext.pushContext(m_nodeContext);
            try {
                m_rows.forEach(r -> writeRowIntoCursor(r));
            } finally {
                NodeContext.removeLastContext();
                m_numPendingBatches.release();
            }
        }
    }
}
