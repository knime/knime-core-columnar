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
 *   3 Nov 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar;

import java.io.Closeable;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ResourceLeakDetector {

    public static final class ResourceWithRelease {

        private final Runnable m_release;

        public ResourceWithRelease(final AutoCloseable closeable) {
            this(closeable, c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    LOGGER.error("Error when attempting to close open closeable.", e);
                }
            });
        }

        public <R> ResourceWithRelease(final R resource, final Consumer<R> release) {
            m_release = () -> release.accept(resource);
        }

        public void release() {
            m_release.run();
        }

    }

    /**
     * Implementation of a {@link PhantomReference} for referent objects that hold resources (e.g., {@link AutoCloseable
     * AutoCloseables}). It can be used to make sure that if the referent fails to release its resources, the resources
     * will be released at some point after the referent has been reclaimed by the garbage collector.
     *
     * <p>
     * The lifecycle and suggested usage of this class is as follows: A new Finalizer can be created for a referent and
     * should be closed by the referent when it releases its resources. If left unclosed, the Finalizer will be enqueued
     * in a {@link ReferenceQueue} once the referent has been reclaimed by the garbage collector. This queue is
     * periodically polled. If any open resources of reclaimed referents are detected, they are released.
     *
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    public final class Finalizer extends PhantomReference<Object> implements Closeable {

        private String stackTraceToString(final StackTraceElement[] stack) {
            return Arrays.stream(stack).map(StackTraceElement::toString).collect(Collectors.joining("\n  "));
        }

        private final String m_referentName;

        private final ResourceWithRelease[] m_resources;

        private final AtomicBoolean m_closed = new AtomicBoolean();

        private final List<String> m_stackTraces;

        private Finalizer(final Object referent, final ResourceWithRelease... resources) {
            super(referent, m_enqueuedFinalizers);
            m_resources = resources;
            if (VERBOSE) {
                m_referentName = referent.getClass().getSimpleName();
                m_stackTraces = new ArrayList<>();
                addStackTrace();
            } else {
                m_referentName = null;
                m_stackTraces = null;
            }
        }

        /**
         * Close this Finalizer. Should be invoked by the referent when it releases its resource.
         */
        @Override
        public void close() {
            m_closed.set(true);
            m_openFinalizers.remove(this);
        }

        /**
         * A method that can be called for determining whether this Finalizer has already been closed.
         *
         * @return true, if this Finalizer has already been closed; false otherwise
         */
        public boolean isClosed() {
            return m_closed.get();
        }

        public void addStackTrace() {
            if (VERBOSE) {
                final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                m_stackTraces.add(IntStream.range(2, stackTrace.length).mapToObj(i -> stackTrace[i])
                    .map(StackTraceElement::toString).collect(Collectors.joining("\n  ")));
            }
        }

        public void releaseResources() {
            if (!isClosed()) {
                for (final ResourceWithRelease resource : m_resources) {
                    resource.release();
                }
            }
            close();
        }

        /**
         * If this Finalizer is unclosed, close it, release the resource held by the referent object, and log some
         * output for detecting and debugging the potential resource leak.
         */
        public void releaseResourcesAndLogOutput() {
            if (!isClosed()) {
                if (VERBOSE) {
                    LOGGER.debug("Resource leak detected: Reclaimed {} did not release its resources. Releasing now.",
                        m_referentName);
                    addStackTrace();
                    LOGGER.debug("Call stacks:");
                    for (String stackTrace : m_stackTraces) {
                        LOGGER.debug(stackTrace);
                    }
                }
                for (final ResourceWithRelease resource : m_resources) {
                    resource.release();
                }
            }
            close();
        }

    }

    private static final String VERBOSE_PROPERTY = "knime.columnar.verbose";

    private static final boolean VERBOSE = Boolean.getBoolean(VERBOSE_PROPERTY);

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceLeakDetector.class);

    private static final long POLL_PERIOD_MS = 1_000L;

    private static final ResourceLeakDetector INSTANCE = new ResourceLeakDetector();

    public static ResourceLeakDetector getInstance() {
        return INSTANCE;
    }

    private final ScheduledExecutorService m_poller;

    // open finalizers are enqueued here on garbage collection of the referent
    private final ReferenceQueue<Object> m_enqueuedFinalizers;

    // open finalizers are referenced here such that they are not garbage-collected
    private final Set<Finalizer> m_openFinalizers;

    private ResourceLeakDetector() {
        m_poller = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "KNIME-MemoryLeakDetector"));
        m_poller.scheduleAtFixedRate(this::poll, POLL_PERIOD_MS, POLL_PERIOD_MS, TimeUnit.MILLISECONDS);
        m_enqueuedFinalizers = new ReferenceQueue<>();
        m_openFinalizers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public Finalizer createFinalizer(final Object referent, final AutoCloseable... closeables) {
        return createFinalizer(referent,
            Arrays.stream(closeables).map(ResourceWithRelease::new).toArray(ResourceWithRelease[]::new));
    }

    /**
     * @param referent referent object that holds a resource
     * @param resource the resource which should be released once the referent has been reclaimed by the garbage
     *            collector
     * @param release the operation that should be applied to release the resource
     *
     * @return a new Finalizer that, if left unclosed itself, will release the resource held by the referent once the
     *         garbage collector reclaims the referent
     */
    public Finalizer createFinalizer(final Object referent, final ResourceWithRelease... resources) {
        final Finalizer dc = new Finalizer(referent, resources);
        m_openFinalizers.add(dc);
        return dc;
    }

    public void clear() {
        m_openFinalizers.clear();
    }

    public int getNumOpenFinalizers() {
        return m_openFinalizers.size();
    }

    @SuppressWarnings("resource")
    public void poll() {
        Finalizer finalizer;
        while ((finalizer = (Finalizer)m_enqueuedFinalizers.poll()) != null) {
            finalizer.releaseResourcesAndLogOutput();
            finalizer.clear();
        }
    }

}
