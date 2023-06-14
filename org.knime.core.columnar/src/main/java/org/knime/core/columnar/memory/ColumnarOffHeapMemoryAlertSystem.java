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
 *   Jun 14, 2023 (benjamin): created
 */
package org.knime.core.columnar.memory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A system for alerting listeners that hold off-heap data that the off-heap memory limit is reached and they should
 * release their data.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class ColumnarOffHeapMemoryAlertSystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarOffHeapMemoryAlertSystem.class);

    /** The singleton instance of the {@link ColumnarOffHeapMemoryAlertSystem} */
    public static final ColumnarOffHeapMemoryAlertSystem INSTANCE = new ColumnarOffHeapMemoryAlertSystem();

    /** The listeners in thread-safe set **/
    private final Set<OffHeapMemoryAlertListener> m_listeners = ConcurrentHashMap.newKeySet();

    /** The last result - used as a return value if another thread executed the memory alert */
    private final AtomicBoolean m_lastReleaseResult = new AtomicBoolean();

    /** Semaphore for blocking access to sending the memory alert - blocked threads will use the last result */
    private final Semaphore m_semaphore = new Semaphore(1);

    private ColumnarOffHeapMemoryAlertSystem() {
    }

    /**
     * Send a memory alert to all listeners. If multiple treads call this method at the same time, the listeners will
     * only be called once.
     *
     * @return <code>true<code> if at least one listener released some resources
     * @throws InterruptedException if another thread calls the listener and this thread is interrupted waiting on the
     *             other thread
     */
    public boolean sendMemoryAlert() throws InterruptedException {
        // TODO is a deadlock possible?
        // 1. Memory alert is called
        // 2. A listener needs to allocate memory to release memory (for example to run compression before serializing data to disc)
        // 3. While allocating another memory alert is triggered
        // 4. The new memory alert cannot acquire the semaphore and waits for itself?

        if (m_semaphore.tryAcquire()) {
            // No other thread is running:
            // This thread calls the memory alert on the listeners
            LOGGER.debug("Received off-heap memory alert. Calling listeners to release memory.");
            var result = callMemoryAlertForListeners();
            m_lastReleaseResult.set(result);
            m_semaphore.release();
            LOGGER.debug(result ? "Memory was released." : "No memory was released");
            return result;
        } else {
            // Another thread already runs the memory alert:
            // Wait for the other thread and use its result
            LOGGER.debug("Received off-heap memory alert. Listeners are already beeing notified by another thread.");
            m_semaphore.acquire();
            var result = m_lastReleaseResult.get();
            m_semaphore.release();
            return result;
        }

    }

    /** Loop over the listeners and call {@link OffHeapMemoryAlertListener#memoryAlert()} for each */
    private boolean callMemoryAlertForListeners() {
        var memoryReleased = false;
        // NB: If listeners are added by another thread they will not be called but I will not cause exceptions
        for (var listener : m_listeners) {
            memoryReleased |= listener.memoryAlert();
        }
        return memoryReleased;
    }

    /**
     * Add the given listener such that it will be called on a memory alert.
     *
     * @param listener
     */
    public void addMemoryListener(final OffHeapMemoryAlertListener listener) {
        m_listeners.add(listener);
    }

    /**
     * Remove the given listener which was added with {@link #addMemoryListener(OffHeapMemoryAlertListener)} before,
     *
     * @param listener
     */
    public void removeMemoryListener(final OffHeapMemoryAlertListener listener) {
        m_listeners.remove(listener);
    }

    /** Interface for listeners on the {@link ColumnarOffHeapMemoryAlertSystem}. */
    public interface OffHeapMemoryAlertListener {

        /**
         * This method is called if the off-heap memory is low. Listeners should release resources. If new off-heap
         * allocations would exceed the memory limit they are blocked until this method returns.
         *
         * @return <code>true<code> if the listener released some resources
         */
        boolean memoryAlert();
    }
}
