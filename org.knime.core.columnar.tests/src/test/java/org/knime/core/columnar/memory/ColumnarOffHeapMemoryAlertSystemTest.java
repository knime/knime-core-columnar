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
 *   Mar 24, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.knime.core.columnar.memory.ColumnarOffHeapMemoryAlertSystem.OffHeapMemoryAlertListener;

/**
 * Tests {@link ColumnarOffHeapMemoryAlertSystem}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("static-method")
final class ColumnarOffHeapMemoryAlertSystemTest {

    @Test
    void testSimpleMemoryAlert() throws InterruptedException {
        var alerts = new AtomicInteger(0);
        OffHeapMemoryAlertListener countingListener = () -> {
            alerts.incrementAndGet();
            return false;
        };

        var called = new AtomicBoolean();
        OffHeapMemoryAlertListener clearingListener = () -> {
            return called.getAndSet(true);
        };

        ColumnarOffHeapMemoryAlertSystem.INSTANCE.addMemoryListener(countingListener);
        ColumnarOffHeapMemoryAlertSystem.INSTANCE.addMemoryListener(clearingListener);
        var cleared = ColumnarOffHeapMemoryAlertSystem.INSTANCE.sendMemoryAlert();
        assertFalse("Must not return cleared on the first call", cleared);
        assertEquals("Exactly 1 alert must be sent to the listeners", 1, alerts.get());

        cleared = ColumnarOffHeapMemoryAlertSystem.INSTANCE.sendMemoryAlert();
        assertTrue("Must return cleared on the second call", cleared);
        assertEquals("Exactly 2 alerts must be sent to the listeners", 2, alerts.get());

        ColumnarOffHeapMemoryAlertSystem.INSTANCE.removeMemoryListener(countingListener);
        ColumnarOffHeapMemoryAlertSystem.INSTANCE.removeMemoryListener(clearingListener);
    }

    @Test
    void testMemoryAlertFromMultipleThreads() throws InterruptedException {

        // Counting the memory alerts
        var alerts = new AtomicInteger(0);
        OffHeapMemoryAlertListener countingListener = () -> {
            try {
                // Wait until all threads started
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                fail("Thread got interrupted unexpectedly");
            }
            alerts.incrementAndGet();
            return false;
        };
        ColumnarOffHeapMemoryAlertSystem.INSTANCE.addMemoryListener(countingListener);

        // Setup the threads
        Runnable memoryAlertRunnable = () -> {
            try {
                ColumnarOffHeapMemoryAlertSystem.INSTANCE.sendMemoryAlert();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                fail("Thread got interrupted unexpectedly");
            }
        };
        final Thread threadA = new Thread(memoryAlertRunnable);
        final Thread threadB = new Thread(memoryAlertRunnable);
        final Thread threadC = new Thread(memoryAlertRunnable);

        threadA.start();
        threadB.start();
        threadC.start();

        threadA.join();
        threadB.join();
        threadC.join();

        assertEquals("Exactly 1 alert must be sent to the listeners", 1, alerts.get());

        ColumnarOffHeapMemoryAlertSystem.INSTANCE.removeMemoryListener(countingListener);
    }
}
