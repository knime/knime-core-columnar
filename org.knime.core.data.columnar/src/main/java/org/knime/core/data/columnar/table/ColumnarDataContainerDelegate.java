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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.mutable.MutableLong;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.BlobSupportDataRow;
import org.knime.core.data.container.Buffer;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.workflow.NodeContext;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarDataContainerDelegate implements DataContainerDelegate {

    /** The executor, which runs the IO tasks. Currently used only while writing rows. */
    static final ThreadPoolExecutor ASYNC_EXECUTORS;

    private static final int MAX_NUM_THREADS;

    /**
     * The cache size for asynchronous table writing. It's the number of rows that are kept in memory before handing it
     * to the writer routines. The default value can be changed using the java property
     * {@link KNIMEConstants#PROPERTY_ASYNC_WRITE_CACHE_SIZE}.
     *
     * @deprecated access via {@link DataContainerSettings#getDefault()}
     */
    @Deprecated
    static final int ASYNC_CACHE_SIZE;

    /**
     * Whether to use synchronous IO while adding rows to a buffer or reading from an file iterator. The default value
     * can be changed by setting the appropriate java property {@link KNIMEConstants#PROPERTY_SYNCHRONOUS_IO} at
     * startup.
     *
     * @deprecated access via {@link DataContainerSettings#getDefault()}
     */
    @Deprecated
    static final boolean SYNCHRONOUS_IO;

    /**
     * The default value for initializing the domain.
     *
     * @deprecated access via {@link DataContainerSettings#getDefault()}
     */
    @Deprecated
    static final boolean INIT_DOMAIN;

    static {
        final DataContainerSettings defaults = DataContainerSettings.getDefault();
        ASYNC_CACHE_SIZE = defaults.getRowBatchSize();
        SYNCHRONOUS_IO = defaults.isForceSequentialRowHandling();
        INIT_DOMAIN = defaults.getInitializeDomain();
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

    private final AtomicBoolean m_cleared = new AtomicBoolean();

    private final AtomicBoolean m_closed = new AtomicBoolean();

    private static final int BATCH_SIZE = 100;

    private long m_curBatchIdx = 0;

    private List<DataRow> m_curBatch = new ArrayList<>(BATCH_SIZE);

    // TODO clear if memory alert occurs
    private final List<DataRow> m_allRows = new ArrayList<>();

    /**
     * The index of the pending batch, i.e., the index of the next batch that has to be forwarded to the {@link Buffer}.
     */
    private final MutableLong m_pendingBatchIdx = new MutableLong();

    /** The write throwable indicating that asynchronous writing failed. */
    private AtomicReference<Throwable> m_writeThrowable = new AtomicReference<>();

    /**
     * Semaphore used to block the container until all {@link #m_maxNumThreads} {@link ContainerRunnable} are finished.
     */
    private final Semaphore m_numActiveContRunnables = new Semaphore(ASYNC_EXECUTORS.getMaximumPoolSize());

    /**
     * Semaphore used to block the producer in case that we have more than {@link #m_maxNumThreads} batches waiting to
     * be handed over to the {@link Buffer}.
     */
    private final Semaphore m_numPendingBatches = new Semaphore(ASYNC_EXECUTORS.getMaximumPoolSize());

    // Whether we are still in the closing operation.
    // We still need to allow adding rows because that could happen asynchronously,
    // and if some serialization was triggered in the close() method, we prevent
    // re-triggering this by setting m_closed=true but set m_closing=true as well
    // to allow serialization to finish.
    private final AtomicBoolean m_closing = new AtomicBoolean();

    private long m_size;

    private UnsavedColumnarContainerTable m_containerTable;

    ColumnarDataContainerDelegate(final DataTableSpec spec, final ColumnarRowContainer container) {
        m_delegateContainer = container;
        m_delegateCursor = container.createCursor();
        m_spec = spec;
        m_numColumns = m_spec.getNumColumns();
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

    @Override
    public void addRowToTable(final DataRow row) {
//        Objects.requireNonNull(row);
//        if (row.getNumCells() != m_numColumns) {
//            throw new IllegalArgumentException(
//                String.format("Cell count in row \"%s\" is not equal to length of column names array: %d vs. %d",
//                    row.getKey().toString(), row.getNumCells(), m_spec.getNumColumns()));
//        }
//        if (m_closed.get() && !m_closing.get()) {
//            throw new IllegalStateException("Container delegate has already been closed.");
//        }
//        if (m_cleared.get()) {
//            throw new IllegalStateException("Container delegate has already been cleared.");
//        }
        /* As per contract, this method should also throw an unchecked DuplicateKeyException if a row's key has already
         * been added. Here, this is done asynchronously and taken care of by the delegate ColumnarRowContainer and its
         * underlying DomainColumnStore. */

        // TODO handle low memory

        m_curBatch.add(row);
        m_allRows.add(row);
        if (m_curBatch.size() == BATCH_SIZE) {
            try {
                submit();
            } catch (InterruptedException ex) {
                // FIXME ...
                throw new IllegalStateException(ex);
            }
        }

//        writeRowIntoCursor(row);
        m_size++;
    }

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
     * Submits the current batch to the {@link #ASYNC_EXECUTORS} service.
     *
     * @throws InterruptedException if an interrupted occured
     */
    private void submit() throws InterruptedException {
        // wait until we are allowed to submit a new runnable
        m_numPendingBatches.acquire();
        m_numActiveContRunnables.acquire();
        // poll can only return null if we never had #nThreads ContainerRunnables at the same
        // time queued for execution or none of the already submitted Runnables has already finished
        // it's computation
//        DataTableDomainCreator domainCreator = m_domainUpdaterPool.poll();
//        if (domainCreator == null) {
//            domainCreator = new DataTableDomainCreator(m_domainCreator);
//            domainCreator.setMaxPossibleValues(m_domainCreator.getMaxPossibleValues());
//        }
        final var batch = m_curBatch;
        ASYNC_EXECUTORS.execute(new ContainerRunnable(batch, m_curBatchIdx++));
        // reset batch
        m_curBatch = new ArrayList<>(BATCH_SIZE);
    }

    @Override
    public ContainerTable getTable() {
        if (!m_closed.get()) {
            throw new IllegalStateException("getTable() can only be called after close() was called.");
        }
        return m_containerTable;
    }

    @Override
    public void close() {
        if (!m_closed.getAndSet(true)) {
            m_closing.set(true);

            submitLastBatchAndWait();
            m_containerTable = m_delegateContainer.finishInternal();
            // TODO only set if not already cleaned up due to memory alert
            m_containerTable.setDataRows(m_allRows);
            m_delegateCursor.close();
            m_closing.set(false);
        }
    }

    private void submitLastBatchAndWait() {
        try {
            submit();
            m_numActiveContRunnables.acquire(MAX_NUM_THREADS);
            m_numActiveContRunnables.release(MAX_NUM_THREADS);
        } catch (InterruptedException ex) {
            // FIXME
            throw new IllegalStateException(ex);
        }

    }

    @Override
    public void clear() {
        if (!m_cleared.getAndSet(true)) {
            if (m_containerTable != null) {
                m_containerTable.clear();
            } else {
                m_delegateCursor.close();
                m_delegateContainer.close();
            }
        }
    }

    /** Map storing those rows that still need to be forwarded to the {@link Buffer}. */
    private final Map<Long, List<DataRow>> m_pendingBatchMap = new ConcurrentHashMap<>();

    private final class ContainerRunnable implements Runnable {

        /** The batch of rows to be processed. */
        private final List<DataRow> m_rows;

        /** The current batch index. */
        private final long m_batchIdx;

        private final NodeContext m_nodeContext;

        /**
         * Constructor.
         *
         * @param domainCreator the domain creator
         * @param rows the batch of rows to be processed
         * @param batchIdx the batch index
         */
        ContainerRunnable(final List<DataRow> rows, final long batchIdx) {
            m_rows = rows;
            m_batchIdx = batchIdx;
            /**
             * The node context may be null if the DataContainer has been created outside of a node's context (e.g., in
             * unit tests). This is also the reason why this class does not extend the RunnableWithContext class.
             */
            m_nodeContext = NodeContext.getContext();
        }

        @Override
        public final void run() {
            NodeContext.pushContext(m_nodeContext);
            try {
                if (m_writeThrowable.get() == null) {
                    boolean addRows;
                    synchronized (m_pendingBatchIdx) {
                        addRows = m_batchIdx == m_pendingBatchIdx.longValue();
                        if (!addRows) {
                            m_pendingBatchMap.put(m_batchIdx, m_rows);
                        }
                    }
                    if (addRows) {
                        addRows(m_rows);
                        m_numPendingBatches.release();
                        while (isNextPendingBatchExistent()) {
                            addRows(m_pendingBatchMap.remove(m_pendingBatchIdx.longValue()));
                            m_numPendingBatches.release();
                        }
                    }

                }
            } catch (final Throwable t) {
                m_writeThrowable.compareAndSet(null, t);
                // Potential deadlock cause by the following scenario.
                // Initial condition:
                //      1. ContainerRunnables:
                //          1.1 The max number of container runnables are submitted to the pool
                //          1.2 No batch has been handed over to the buffer (m_numPendingBatches no available permits)
                //      2. Producer:
                //          2.1. Hasn't seen an exception so far wants to submit another batch and tries to acquire a
                //              permit from m_numPendingBatches
                //      3. The container runnable whose index equals the current pending batch index crashes before
                //          it can hand anything to the buffer and finally give its permit back to m_numPendingBatches
                // Result: The producer waits forever to acquire a permit from m_numPendingBatches. Hence, we have to
                //          release at least one permit here
                // Note: The producer will submit another ContainerRunnable, however this will do nothing as
                //          m_writeThrowable.get != null
                if (m_batchIdx == m_pendingBatchIdx.longValue()) {
                    m_numPendingBatches.release();
                }
            } finally {
                m_numActiveContRunnables.release();
                NodeContext.removeLastContext();
            }
        }

        /**
         * Forwards the given list of {@link BlobSupportDataRow} to the buffer
         *
         * @param blobRows the rows to be forwarded to the buffer
         * @throws IOException - if the buffer cannot write the rows to disc
         */
        private void addRows(final List<DataRow> rows) throws IOException {
            rows.forEach(r -> writeRowIntoCursor(r));
        }

        /**
         * Checks whether there is another batch of {@link BlobSupportDataRow} existent that has to be forwarded to the
         * buffer.
         *
         * @return {@code true} if another batch has to be forwarded to the buffer, {@code false} otherwise
         *
         */
        private boolean isNextPendingBatchExistent() {
            synchronized (m_pendingBatchIdx) {
                m_pendingBatchIdx.increment();
                return m_pendingBatchMap.containsKey(m_pendingBatchIdx.longValue());
            }
        }

    }

}
