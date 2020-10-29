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
package org.knime.core.data.columnar.table;

import java.io.Closeable;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.knime.core.node.NodeLogger;

/**
 * Implementation of a {@link PhantomReference} for referent objects that hold resources (e.g., {@link AutoCloseable
 * AutoCloseables}). It can be used to make sure that if the referent fails to release its resources, the resources will
 * be released at some point after the referent has been reclaimed by the garbage collector.
 *
 * <p>
 * The lifecycle and suggested usage of this class is as follows: A new Finalizer can be created for a referent and
 * should be closed by the referent when it releases its resources. If left unclosed, the Finalizer will be enqueued in
 * a {@link ReferenceQueue} once the referent has been reclaimed by the garbage collector. This queue is periodically
 * polled. If any open resources of reclaimed referents are detected, they are released.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @param <T> the type of the resource held by the referent
 */
final class Finalizer<T> extends PhantomReference<Object> implements Closeable {

    private static final String VERBOSE_PROPERTY = "knime.columnar.verbose";

    private static final boolean VERBOSE = Boolean.getBoolean(VERBOSE_PROPERTY);

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Finalizer.class);

    private static final long POLL_PERIOD_MS = 1_000L;

    private static final ScheduledExecutorService POLLER =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "KNIME-MemoryLeakDetector"));
    static {
        POLLER.scheduleAtFixedRate(Finalizer::poll, POLL_PERIOD_MS, POLL_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    // open finalizers are enqueued here on garbage collection of the referent
    static final ReferenceQueue<Object> ENQUEUED_FINALIZERS = new ReferenceQueue<>();

    // open finalizers are referenced here such that they are not garbage-collected
    static final Set<Finalizer<?>> OPEN_FINALIZERS = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @SuppressWarnings("resource")
    static void poll() {
        Finalizer<?> finalizer;
        while ((finalizer = (Finalizer<?>)ENQUEUED_FINALIZERS.poll()) != null) {
            finalizer.releaseResourcesAndLogOutput();
            finalizer.clear();
        }
    }

    private static String stackTraceToString(final StackTraceElement[] stack) {
        return Arrays.stream(stack).map(StackTraceElement::toString).collect(Collectors.joining("\n  "));
    }

    /**
     * @param <T> the type of the AutoCloseable held by the referent
     * @param referent referent object that holds an AutoCloseable
     * @param closeable the AutoCloseable which should be closed once the referent has been reclaimed by the garbage
     *            collector
     *
     * @return a new Finalizer that, if left unclosed itself, will close the AutoCloseable held by the referent once the
     *         garbage collector reclaims the referent
     */
    static <T extends AutoCloseable> Finalizer<T> create(final Object referent, final T closeable) {
        return Finalizer.create(referent, closeable, c -> {
            try {
                c.close();
            } catch (Exception e) {
                LOGGER.error("Error when attempting to close open closeable.", e);
            }
        }, "close");
    }

    /**
     * @param <T> the type of the resource held by the referent
     * @param referent referent object that holds a resource
     * @param resource the resource which should be released once the referent has been reclaimed by the garbage
     *            collector
     * @param release the operation that should be applied to release the resource
     * @param releaseName a human-readable String representation of the release operation
     *
     * @return a new Finalizer that, if left unclosed itself, will release the resource held by the referent once the
     *         garbage collector reclaims the referent
     */
    static <T> Finalizer<T> create(final Object referent, final T resource, final Consumer<T> release,
        final String releaseName) {
        final Finalizer<T> dc = new Finalizer<>(referent, resource, release, releaseName);
        OPEN_FINALIZERS.add(dc);
        return dc;
    }

    private final String m_referentName;

    private final T m_resource;

    private final Consumer<T> m_release;

    private final String m_releaseName;

    private final AtomicBoolean m_closed = new AtomicBoolean();

    private final String m_stackTraceAtConstructionTime =
        VERBOSE ? stackTraceToString(Thread.currentThread().getStackTrace()) : null;

    private Finalizer(final Object referent, final T closeable, final Consumer<T> apply, final String resourceName) {
        super(referent, ENQUEUED_FINALIZERS);
        m_referentName = VERBOSE ? referent.getClass().getSimpleName() : null;
        m_resource = closeable;
        m_release = apply;
        m_releaseName = VERBOSE ? resourceName : null;
    }

    /**
     * Close this Finalizer. Should be invoked by the referent when it releases its resource.
     */
    @Override
    public void close() {
        m_closed.set(true);
        OPEN_FINALIZERS.remove(this);
    }

    /**
     * A method that can be called for determining whether this Finalizer has already been closed.
     *
     * @return true, if this Finalizer has already been closed; false otherwise
     */
    boolean isClosed() {
        return m_closed.get();
    }

    /**
     * If this Finalizer is unclosed, close it, release the resource held by the referent object, and log some output
     * for detecting and debugging the potential resource leak.
     */
    void releaseResourcesAndLogOutput() {
        if (!isClosed()) {
            if (VERBOSE) {
                final String resourceName = m_resource.getClass().getSimpleName();
                LOGGER.debugWithFormat("Resource leak detected: Reclaimed %s did not %s its %s. Will %s %s now.",
                    m_referentName, m_releaseName, resourceName, m_releaseName, resourceName);
                LOGGER.debugWithFormat("Construction time call stack: %s", m_stackTraceAtConstructionTime);
                LOGGER.debugWithFormat("Current call stack: %s",
                    stackTraceToString(Thread.currentThread().getStackTrace()));
            }
            m_release.accept(m_resource);
        }
        close();
    }

}
