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
 *   Oct 21, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.object;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

/**
 * Similar to {@link CountDownLatch} and {@link Phaser}, this is a latch that can be counted up and down. Await will
 * wait for its count to become zero from above. Increasing from below to zero will not trigger await to return.
 * Cannot be reused after the count has reached zero once.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class CountUpDownLatch {

    private long m_count;

    private Phaser m_phaser = new Phaser(1);

    /**
     * Create a {@link CountUpDownLatch} with an initial count.
     *
     * @param initialCount Initial count, should not be zero or await() will return immediately.
     */
    public CountUpDownLatch(final long initialCount) {
        m_count = initialCount;
        if (initialCount == 0) {
            m_phaser.arriveAndDeregister();
        }
    }

    /**
     * Increase the count.
     */
    public synchronized void countUp() {
        m_count++;
    }

    /**
     * Decrease the count, if it becomes zero, await will return.
     */
    public synchronized void countDown() {
        m_count--;
        if (m_count == 0) {
            m_phaser.arriveAndDeregister();
        }
    }

    /**
     * Wait for the count to become zero. Not interruptible.
     */
    public void await() {
        m_phaser.awaitAdvance(0);
    }

    /**
     * Wait for the count to become zero.
     *
     * @throws InterruptedException
     */
    public void awaitInterruptibly() throws InterruptedException {
        m_phaser.awaitAdvanceInterruptibly(0);
    }

}
