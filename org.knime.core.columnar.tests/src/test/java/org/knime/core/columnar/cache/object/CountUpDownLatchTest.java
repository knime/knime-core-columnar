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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class CountUpDownLatchTest {

    @Test(timeout = 100)
    public void testCountUpDownLatchAwaitDoesReturn() { // NOSONAR
        final var latch = new CountUpDownLatch(1);
        latch.countUp();
        latch.countDown();
        latch.countDown();
        latch.await();
    }

    @Test(timeout = 100)
    public void testCountUpDownLatchAwaitReturnsImmediately() { // NOSONAR
        final var latch = new CountUpDownLatch(0);
        latch.await();
    }

    @Test(timeout = 100)
    public void testCountUpDownLatchAwaitBlocks() {
        final var latch = new CountUpDownLatch(1);
        final var reachedZero = new AtomicBoolean(false);

        final var exec = Executors.newSingleThreadExecutor();
        exec.submit(() -> {
            try {
                Thread.sleep(10); //NOSONAR
            } catch (InterruptedException ex) {
                // meh, just for testing
            }
            latch.countUp();
            latch.countUp();
            latch.countDown();
            latch.countDown();
            latch.countUp();
            latch.countUp();
            latch.countDown();
            latch.countDown();
            reachedZero.set(true);
            latch.countDown();
        });

        latch.await();
        assertTrue(reachedZero.get());
    }

    @Test(timeout = 100)
    public void testCountUpDownLatchDoesNotReturnWhenComingFromBelowZero() {
        final var latch = new CountUpDownLatch(-1);
        final var reachedZero = new AtomicBoolean(false);

        final var exec = Executors.newSingleThreadExecutor();
        exec.submit(() -> {
            try {
                Thread.sleep(10); //NOSONAR
            } catch (InterruptedException ex) {
                // meh, just for testing
            }
            latch.countUp(); // 0, but not returning yet
            latch.countUp(); // 1
            latch.countUp(); // 2
            latch.countUp(); // 3
            latch.countDown(); // 2
            latch.countDown(); // 1
            latch.countUp(); // 2
            latch.countUp(); // 3
            latch.countDown(); // 2
            latch.countDown(); // 1
            reachedZero.set(true);
            latch.countDown(); // 0 ->baam!
        });

        latch.await();
        assertTrue(reachedZero.get());
    }
}
