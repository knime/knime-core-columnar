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
 *   27 Aug 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.phantom;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Implementation of a {@link PhantomReference} for referent objects that work with a {@link Closeable}. It can be used
 * to make sure that if the referent fails to close its Closeable, the Closeable will be closed at some point after the
 * referent has been reclaimed by the garbage collector. It can be used, for instance, to detect memory leaks.
 *
 * <p>
 * The lifecycle and suggested usage of this class is as follows: A new DelegateCloser can be
 * {@link #create(Object, Closeable, String) created} for a referent and should be closed {@link #close() closed} by the
 * referent along with its Closeable. If left unclosed, the DelegateCloser will be enqueued in a {@link ReferenceQueue}
 * once the referent has been reclaimed by the garbage collector. This queue is periodically polled. If any unclosed
 * Closeables are detected, they are closed and an error is logged.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class CloseableCloser extends PhantomReference<Object> implements Closeable {

    private static final long POLL_PERIOD_MS = 1_000L;

    private static final ScheduledExecutorService POLLER =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "KNIME-MemoryLeakDetector"));
    static {
        POLLER.scheduleAtFixedRate(CloseableCloser::poll, POLL_PERIOD_MS, POLL_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    // delegate closers are enqueued here when an unclosed delegating store is garbage-collected
    static final ReferenceQueue<Object> ENQUEUED_FINALIZERS = new ReferenceQueue<>();

    // delegate closers are referenced here such that they are not garbage-collected
    static final Set<CloseableCloser> OPEN_FINALIZERS = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @SuppressWarnings("resource")
    static void poll() {
        CloseableCloser closer;
        while ((closer = (CloseableCloser)ENQUEUED_FINALIZERS.poll()) != null) {
            try {
                closer.closeCloseableAndSelf();
            } catch (IOException e) {
                System.err.println(String.format("Error when attempting to close open resource: %s.", e.getMessage()));
                System.err.println(String.format("Stack trace: %s", stackTraceToString(e.getStackTrace())));
            }
            closer.clear();
        }
    }

    private static String stackTraceToString(final StackTraceElement[] stack) {
        return Arrays.stream(stack).map(StackTraceElement::toString).collect(Collectors.joining("\n  "));
    }

    /**
     * @param referent referent object that works with a Closeable
     * @param closeable the Closeable which should be closed once the referent has been reclaimed by the garbage
     *            collector
     * @param name a human-readable String representation of the referent object class
     * @return a new CloseableCloser that, if left unclosed itself, will close the closeable held by the referent once
     *         the garbage collector reclaims the referent
     */
    public static CloseableCloser create(final Object referent, final Closeable closeable, final String name) {
        final CloseableCloser dc = new CloseableCloser(referent, closeable, name);
        OPEN_FINALIZERS.add(dc);
        return dc;
    }

    private final Closeable m_closeable;

    private final AtomicBoolean m_closed = new AtomicBoolean();

    private final String m_resourceName;

    private final String m_stackTraceAtConstructionTime = stackTraceToString(Thread.currentThread().getStackTrace());

    private CloseableCloser(final Object resource, final Closeable closeable, final String resourceName) {
        super(resource, ENQUEUED_FINALIZERS);
        m_closeable = closeable;
        m_resourceName = resourceName;
    }

    @Override
    public void close() {
        m_closed.set(true);
        OPEN_FINALIZERS.remove(this);
    }

    boolean isClosed() {
        return m_closed.get();
    }

    private void closeCloseableAndSelf() throws IOException {
        if (!isClosed()) {
            System.err.println(String.format("%s resource was not closed.", m_resourceName));
            System.err.println(String.format("Construction time call stack: %s", m_stackTraceAtConstructionTime));
            m_closeable.close();
        }
        close();
    }

}
