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
package org.knime.core.columnar.parallel.exec;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public final class WriteTaskExecutor implements Consumer<RowWriteTask>, AutoCloseable {

    private static final ExecutorService EXECUTOR =
        Executors.newFixedThreadPool(4, new ThreadFactory() {
            private final AtomicInteger m_threadNum = new AtomicInteger(0);
            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(r, "KNIME-Columnar-Write-Task-Executor-" + m_threadNum.getAndIncrement());
            }
        });

    // might make this blocking and only allow a limited number of pending tasks
    private final BlockingQueue<RowWriteTask> m_tasks = new LinkedBlockingQueue<>(3);

    private final List<DataWriter> m_columnWriters;

    private Future<?> m_currentTaskFuture;

    private final AtomicBoolean m_open = new AtomicBoolean(true);

    private final AtomicBoolean m_waitingForFinish = new AtomicBoolean(false);

    public WriteTaskExecutor(final List<DataWriter> columnWriters) {
        m_columnWriters = columnWriters;
        m_currentTaskFuture = EXECUTOR.submit(this::scheduleColumnTasks);
    }

    private void scheduleColumnTasks() {
        // null is the poison pill
        while (m_open.get()) {
            try (var task = m_tasks.take()) {
                if (task == RowWriteTask.NULL) {
                    // poison pill
                    return;
                }
                var doneLatch = new CountDownLatch(m_columnWriters.size());
                for (int c = 0; c < m_columnWriters.size(); c++) { //NOSONAR
                    var columnTask = new ColumnWriteTask(task, c, m_columnWriters.get(c), EXECUTOR, doneLatch);
                    EXECUTOR.submit(columnTask);
                }
                if (!doneLatch.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("RowWriteTask not done in time!");
                }
            } catch (InterruptedException interruptWhileWaitingForTask) {
                // TODO can also be happening while we wait for the current task to finish
                // TODO maybe that's just fine?
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for tasks.",
                    interruptWhileWaitingForTask);
            }
        }

    }

    @Override
    public void accept(final RowWriteTask t) {
        if (m_waitingForFinish.get()) {
            throw new IllegalStateException("Waiting for the queue to be finished. No more tasks can be added.");
        }
        try {
            m_tasks.put(t);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Writing has been interrupted.", ex);
        }
    }

    public void await() throws InterruptedException {
        m_waitingForFinish.set(true);
        // insert the poison pill
        m_tasks.put(RowWriteTask.NULL);
        try {
            m_currentTaskFuture.get();
        } catch (ExecutionException ex) {//NOSONAR just a wrapper
            throw new IllegalStateException("Asynchronous writing failed.", ex.getCause());
        }
    }

    @Override
    public void close() {
        if (m_open.getAndSet(false)) {
            m_tasks.forEach(RowWriteTask::close);
            m_currentTaskFuture.cancel(true);
        }
    }

}