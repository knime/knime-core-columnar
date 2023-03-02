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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.core.columnar.parallel.write.GridWriter;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class WriteTaskExecutor<A> implements Consumer<RowTaskBatch<A>>, AutoCloseable {

    // might make this blocking and only allow a limited number of pending tasks
    private final BlockingQueue<RowTaskBatch<A>> m_tasks = new LinkedBlockingQueue<>(3);

    private final List<DataWriter<A>> m_columnWriters;

    private Future<?> m_currentTaskFuture;

    private final AtomicBoolean m_open = new AtomicBoolean(true);

    private final AtomicBoolean m_waitingForFinish = new AtomicBoolean(false);

    private final ExecutorService m_executor;

    private final RowTaskBatch<A> m_poisonPill;

    private final GridWriter<A> m_gridWriter;

    private final UnaryOperator<Callable<Boolean>> m_callableAdapter;

    public WriteTaskExecutor(final GridWriter<A> gridWriter, final ExecutorService executor,
        final RowTaskBatch<A> poisonPill, final UnaryOperator<Callable<Boolean>> callableAdapter) {
        m_gridWriter = gridWriter;
        m_poisonPill = poisonPill;
        m_executor = executor;
        m_columnWriters = IntStream.range(0, gridWriter.numWriters())//
            .mapToObj(gridWriter::getDataWriter)//
            .collect(Collectors.toList());
        m_currentTaskFuture = m_executor.submit(this::scheduleColumnTasks);
        m_callableAdapter = callableAdapter;
    }

    private static class Take<T> implements ForkJoinPool.ManagedBlocker
    {
        private BlockingQueue<T> queue;

        private volatile T element;

        Take(final BlockingQueue<T> queue) {
            this.queue = queue;
        }

        @Override
        public boolean block() throws InterruptedException {
            if ( element == null ) {
                element = queue.take();
            }
            return true;
        }

        @Override
        public boolean isReleasable() {
            if ( element != null ) {
                return true;
            }
            element = queue.poll();
            return element != null;
        }

        T get()
        {
            final T result = element;
            element = null;
            return result;
        }
    }

    private void scheduleColumnTasks()
    {
        final Take<RowTaskBatch<A>> takeTask = new Take<>(m_tasks);
        while (m_open.get()) {
            try {
                ForkJoinPool.managedBlock(takeTask);
                try (var task = takeTask.get()) {
                    if (task == m_poisonPill) {
                        // poison pill
                        return;
                    }
                    final List<ForkJoinTask<Boolean>> columnTasks = new ArrayList<>();
                    for (int c = 0; c < m_columnWriters.size(); c++) { //NOSONAR
                        var writer = m_columnWriters.get(c);
                        var columnWriteTask = task.createColumnTask(c, writer.getAccess());
                        columnTasks.add(ForkJoinTask.adapt(m_callableAdapter.apply(new ColumnWriteTaskRunner(writer, columnWriteTask))));
                    }
                    ForkJoinTask.invokeAll(columnTasks);
                    while (columnTasks.stream().anyMatch(columnTask -> !columnTask.join())) {
                        columnTasks.forEach(ForkJoinTask::reinitialize);
                        ForkJoinTask.invokeAll(columnTasks);
                    }
                }
            } catch (InterruptedException interruptWhileWaitingForTask) {
                // TODO can also be happening while we wait for the current task to finish
                // TODO maybe that's just fine?
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for tasks.", interruptWhileWaitingForTask);
            }
        }
    }

    @Override
    public void accept(final RowTaskBatch<A> t) {
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
        m_tasks.put(m_poisonPill);
        try {
            m_currentTaskFuture.get();
        } catch (ExecutionException ex) {//NOSONAR just a wrapper
            throw new IllegalStateException("Asynchronous writing failed.", ex.getCause());
        }
        try {
            m_gridWriter.finishLastBatch();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to finish last batch.", ex);
        }
    }

    @Override
    public void close() {
        if (m_open.getAndSet(false)) {
            m_tasks.forEach(RowTaskBatch::close);
            m_currentTaskFuture.cancel(true);
        }
    }

    private final class ColumnWriteTaskRunner implements Callable<Boolean> {

        private int m_readIdx;

        private final ColumnTask m_task;

        private final DataWriter<A> m_writer;

        ColumnWriteTaskRunner(final DataWriter<A> writer, final ColumnTask task) {
            m_task = task;
            m_writer = writer;
        }

        @Override
        public Boolean call() {
            for (; m_readIdx < m_task.size(); m_readIdx++) { //NOSONAR
                if (m_writer.canWrite()) {
                    m_task.performSubtask(m_readIdx);
                    m_writer.advance();
                } else {
                    return false;
                }
            }
            return true;
        }
    }

}