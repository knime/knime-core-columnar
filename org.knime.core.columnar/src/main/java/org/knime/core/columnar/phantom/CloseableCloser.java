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
 *   11 Sep 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.phantom;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A class for handling potentially unclosed {@link AutoCloseable Closeables} in order to detect and address resource
 * leaks.
 *
 * The lifecycle and suggested usage of this class is as follows: A new CloseableCloser can be
 * {@link #CloseableCloser(AutoCloseable) created} and should be {@link #close() closed} alongside the to-be-monitored
 * Closeable. If left unclosed, it can be used to manually close the Closeable and log some output for detecting the
 * resource leak.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class CloseableCloser implements CloseableHandler {

    static String stackTraceToString(final StackTraceElement[] stack) {
        return Arrays.stream(stack).map(StackTraceElement::toString).collect(Collectors.joining("\n  "));
    }

    private final AutoCloseable m_closeable;

    private final AtomicBoolean m_closed = new AtomicBoolean();

    private final String m_resourceName;

    private final String m_stackTraceAtConstructionTime = stackTraceToString(Thread.currentThread().getStackTrace());

    /**
     * @param closeable the closeable that should be handled
     */
    public CloseableCloser(final AutoCloseable closeable) {
        this(closeable, closeable.getClass().getSimpleName());
    }

    /**
     * @param closeable the closeable that should be handled
     * @param resourceName a human-readable String representation of the referent object class
     */
    public CloseableCloser(final AutoCloseable closeable, final String resourceName) {
        m_closeable = closeable;
        m_resourceName = resourceName;
    }

    @Override
    public void close() {
        m_closed.set(true);
    }

    @Override
    public boolean isClosed() {
        return m_closed.get();
    }

    @Override
    public void closeCloseableAndLogOutput() throws Exception {
        if (!isClosed()) {
            m_closeable.close();
            System.err.println(String.format(
                "%s resource was not correctly released by its owner and was only released just now.", m_resourceName));
            System.err.println(String.format("Construction time call stack: %s", m_stackTraceAtConstructionTime));
            System.err.println(
                String.format("Current call stack: %s", stackTraceToString(Thread.currentThread().getStackTrace())));
        }
        close();
    }

}
