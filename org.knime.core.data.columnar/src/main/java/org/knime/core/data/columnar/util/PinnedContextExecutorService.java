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
 *   Apr 11, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.knime.core.node.workflow.NodeContext;

/**
 * {@link ExecutorService} that during its creation obtains the {@link NodeContext} and ensures that all its
 * asynchronous task run with that context.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PinnedContextExecutorService implements ExecutorService {

    private final NodeContext m_context;

    private final ExecutorService m_executor;

    /**
     * Constructor.
     */
    public PinnedContextExecutorService(final ExecutorService executor) {
        m_executor = executor;
        m_context = NodeContext.getContext();
    }

    @Override
    public void execute(final Runnable command) {
        m_executor.execute(wrap(command));
    }

    @Override
    public void shutdown() {
        m_executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return m_executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return m_executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return m_executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        return m_executor.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task) {
        return m_executor.submit(wrap(task));
    }

    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {
        return m_executor.submit(wrap(task), result);
    }

    @Override
    public Future<?> submit(final Runnable task) {
        return m_executor.submit(wrap(task));
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return m_executor.invokeAll(wrap(tasks));
    }

    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout,
        final TimeUnit unit) throws InterruptedException {
        return m_executor.invokeAll(wrap(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        return m_executor.invokeAny(wrap(tasks));
    }

    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return invokeAny(wrap(tasks), timeout, unit);
    }

    private <T> Collection<? extends Callable<T>> wrap(final Collection<? extends Callable<T>> tasks) {
        return tasks.stream().map(this::wrap).collect(Collectors.toList());
    }

    private <T> Callable<T> wrap(final Callable<T> task) {
        return new CallableWithContext<>(task, m_context);
    }

    private Runnable wrap(final Runnable runnable) {
        return new RunnableWithContext(runnable);
    }

    private final class RunnableWithContext implements Runnable {

        private final Runnable m_runnable;

        RunnableWithContext(final Runnable runnable) {
            m_runnable = runnable;
        }

        @Override
        public void run() {
            NodeContext.pushContext(m_context);
            try {
                m_runnable.run();
            } finally {
                NodeContext.removeLastContext();
            }
        }

    }

    public static final class CallableWithContext<T> implements Callable<T> {

        private final Callable<T> m_callable;

        private final NodeContext m_context;

        public CallableWithContext(final Callable<T> callable, final NodeContext context) {
            m_callable = callable;
            m_context = context;
        }

        @Override
        public T call() throws Exception {
            NodeContext.pushContext(m_context);
            try {
                return m_callable.call();
            } finally {
                NodeContext.removeLastContext();
            }
        }

    }

}
