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
package org.knime.core.columnar;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link ReferenceCounter}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("static-method")
final class ReferenceCounterTest {

    private ReferenceCounter m_refCounter;

    @BeforeEach
    void setup() {
        m_refCounter = new ReferenceCounter();
    }

    @Test
    void testSynchronousReferenceCounting() {
        randomRetainRelease(5);
        assertTrue(m_refCounter.release(), "The reference count should now be zero.");
    }

    @Test
    void testRetainAfterFinalRelease() {
        assertTrue(m_refCounter.release(), "The only reference has been released.");
        assertThrows(IllegalStateException.class, m_refCounter::retain);
    }

    @Test
    void testTooManyReleases() {
        assertTrue(m_refCounter.release(), "The only reference has been released.");
        assertThrows(IllegalStateException.class, m_refCounter::release);
    }

    @Test
    void testAsynchronousReferenceCounting() throws Exception {
        var startLatch = new CountDownLatch(1);
        var threadOne = new Thread(() -> waitForStart(startLatch, () -> randomRetainRelease(100)));
        var threadTwo = new Thread(() -> waitForStart(startLatch, () -> randomRetainRelease(100)));

        threadOne.start();
        threadTwo.start();
        startLatch.countDown();

        threadOne.join();
        threadTwo.join();
        assertTrue(m_refCounter.release(), "All references should be released now.");
    }

    private void waitForStart(final CountDownLatch startLatch, final Runnable runnable) {
        try {
            startLatch.await();
        } catch (InterruptedException ex) {
            throw new IllegalStateException("Should not be interrupted.");
        }
        runnable.run();
    }

    private void randomRetainRelease(final int numRetains) {
        var random = new Random();
        int retains = 0;//NOSONAR
        for (int i = 0; i < numRetains;) {//NOSONAR
            if (retains > 0 && random.nextBoolean()) {
                retains--;
                assertFalse(m_refCounter.release(), "There are still at least %s open references.".formatted(retains + 1));
            } else {
                i++;
                retains++;
                m_refCounter.retain();
            }
        }
        for (; retains > 0; retains--) {
            assertFalse(m_refCounter.release(), "There are still at lest %s open references.".formatted(retains + 1));
        }
    }
}
