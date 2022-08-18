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
 *   Apr 11, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.knime.core.data.DataRow;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.util.memory.MemoryAlert;
import org.knime.core.data.util.memory.MemoryAlertListener;
import org.knime.core.data.util.memory.MemoryAlertSystem;

/**
 * RowIterator that uses a thread pool to asynchronously prefetch rows.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PrefetchingRowIterator extends CloseableRowIterator {

    private static final ThreadPoolExecutor EXECUTOR =
        new ThreadPoolExecutor(4, 8, 10L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactory() {
            private final AtomicLong m_threadCount = new AtomicLong();

            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(r, "KNIME-Columnar-Iterator-Prefetching-Thread-" + m_threadCount.getAndIncrement());
            }
        });

    private final CloseableRowIterator m_source;

    private final int m_batchSize;

    private final int m_queueSize;

    private static final class RowBatch {
        final DataRow[] rows;

        final boolean isLastBatch;

        public RowBatch(final DataRow[] rows, final boolean isLastBatch) {
            this.rows = rows;
            this.isLastBatch = isLastBatch;
        }
    }

    private final BlockingDeque<RowBatch> m_batchQueue;

    private final AtomicBoolean m_lowMemory = new AtomicBoolean(false);

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    private final MemoryAlertListener m_memListener;

    /**
     * TODO (TP) javadoc
     */
    private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

    private RowBatch m_currentBatch;

    // switch to synchronous iteration caused by low memory or because source is exhausted
    private boolean m_sync = false;

    private DataRow m_currentRow;

    private int m_currentIndex = -1;

    public PrefetchingRowIterator(final CloseableRowIterator source) {
        m_source = source;

        m_batchSize = 100; // TODO (TP)
        m_queueSize = 3; // TODO (TP)
        m_batchQueue = new LinkedBlockingDeque<>(m_queueSize);

        m_memListener = new MemoryAlertListener() {
            @Override
            protected boolean memoryAlert(final MemoryAlert alert) {
                m_lowMemory.set(true);
                return true; // indicates that this listener should be removed (because once we go into low-memory regime, we stay there.)
            }
        };
        MemoryAlertSystem.getInstanceUncollected().addListener(m_memListener);

        m_sync = true;
        if (!m_sync) {
            for (int i = 0; i < m_queueSize; ++i) {
                enqueuePrefetchBatch();
            }
        }
    }

    private void enqueuePrefetchBatch() {
        m_future = m_future.thenRunAsync(() -> prefetchBatch(), EXECUTOR);
    }

    private void prefetchBatch() {
        final DataRow[] rows = new DataRow[ m_batchSize ];
        for (int i = 0; i < m_batchSize; ++i) {
            if (m_closed.get()) {
                return;
            }
            if (m_lowMemory.get()) {
                m_batchQueue.add(new RowBatch(Arrays.copyOf(rows,  i), true));
                return;
            }
            if (m_source.hasNext()) {
                rows[i] = m_source.next();
            } else {
                m_batchQueue.add(new RowBatch(Arrays.copyOf(rows,  i), true));
                return;
            }
        }
        m_batchQueue.add(new RowBatch(rows, false));
    }

    private DataRow getNextRow() {
        if (m_sync) {
            return m_source.hasNext() ? m_source.next() : null;
        }

        if (m_currentBatch == null) {
            checkAsyncWriteThrowable();
            try {
                m_currentBatch = m_batchQueue.takeFirst();
            } catch (InterruptedException e) {
                throw wrap(e);
            }
            m_currentIndex = 0;
            enqueuePrefetchBatch();
        }

        final DataRow[] rows = m_currentBatch.rows;
        if (m_currentIndex < rows.length) {
            return rows[m_currentIndex++];
        } else {
            // end of current batch
            if (m_currentBatch.isLastBatch) {
                m_sync = true;
            }
            m_currentBatch = null;
            return getNextRow();
        }
    }

    @Override
    public boolean hasNext() {
        if (m_currentRow == null) {
            m_currentRow = getNextRow();
        }
        return m_currentRow != null;
    }

    @Override
    public DataRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        } else {
            var row = m_currentRow;
            m_currentRow = null;
            return row;
        }
    }

    @Override
    public void close() {
        m_closed.set(true);
        waitForAndHandleFuture();
        m_batchQueue.clear();
        m_source.close();
        MemoryAlertSystem.getInstanceUncollected().removeListener(m_memListener);
    }

    /**
     * Check and rethrow exceptions that occurred during asynchronous prefetching
     */
    private void checkAsyncWriteThrowable() {
        if (m_future.isCompletedExceptionally()) {
            waitForAndHandleFuture();
        }
    }

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
        return new RuntimeException("Error while prefetching rows", t);
    }
}
