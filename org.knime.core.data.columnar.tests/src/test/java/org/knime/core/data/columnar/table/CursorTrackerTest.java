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
 *   Feb 4, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.knime.core.table.cursor.Cursor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Contains unit tests for the CursorTracker.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "resource"})
@RunWith(MockitoJUnitRunner.class)
public class CursorTrackerTest {

    @Mock
    private Cursor<Object> m_cursor;

    private CursorTracker<Cursor<Object>> m_tracker;

    @Before
    public void before() {
        m_tracker = CursorTracker.createCursorTracker();
    }

    private static class DummyCursor implements Cursor<Integer> {

        private int m_value = 0;

        @Override
        public void close() throws IOException {

        }

        @Override
        public Integer access() {
            return m_value;
        }

        @Override
        public boolean forward() {
            m_value++;
            return true;
        }

    }

    public void benchmarkFinalizerAwareCosts() throws Exception {
        var warmupRounds = 100_000;
        var benchmarkRounds = 100_000_000;
        var undecorated = new DummyCursor();
        System.out.println("Undecorated:");
        long timeUndecorated = benchmarkForward(undecorated, warmupRounds, benchmarkRounds);

        final var avgUndecorated = ((double)timeUndecorated) / benchmarkRounds;
        System.out.println("Undecorated average time per forward: " + avgUndecorated);

        var decorated = CursorsWithFinalizer.cursor(new DummyCursor(), c -> {});
        System.out.println("Decorated:");
        long timeDecorated = benchmarkForward(decorated, warmupRounds, benchmarkRounds);

        final var avgDecorated = ((double)timeDecorated) / benchmarkRounds;
        System.out.println("Decorated average time per forward: " + avgDecorated);

        System.out.println(avgDecorated - avgUndecorated);
    }

    private static long benchmarkForward(final Cursor<Integer> cursor, final int warmupRounds, final int benchmarkRounds) {
        int warmupSum = 0;
        for (int i = 0; i < warmupRounds; i++) {
            warmupSum += cursor.access();
            cursor.forward();
        }
        System.out.println("Warmupsum: " + warmupSum);

        long startTime = System.currentTimeMillis();
        int benchmarkSum = 0;
        for (int i = 0; i < benchmarkRounds; i++) {
            benchmarkSum += cursor.access();
            cursor.forward();
        }
        System.out.println("Benchmarksum: " + benchmarkSum);
        return System.currentTimeMillis() - startTime;
    }

    @Test
    public void testGcCollectedCursorsAreReleased() throws IOException, InterruptedException {
        m_tracker.createTrackedCursor(() -> m_cursor);
        // force garbage collection so that the cursor returned above is collected
        System.gc();
        Thread.sleep(1000);
        // let the ResourceLeakDetector clean up unreleased resources
        ResourceLeakDetector.getInstance().poll();
        // verify that the underlying cursor was closed
        verify(m_cursor).close();
    }

    @Test
    public void testCloseReleasesResources() throws IOException {
        // hold on to cursor, so that it doesn't get garbage collected
        @SuppressWarnings("unused")
        var cursor = m_tracker.createTrackedCursor(() -> m_cursor);
        m_tracker.close();
        verify(m_cursor).close();
    }

    @SuppressWarnings("unused") // the cursors are assigned to variables to avoid garbage collection
    @Test
    public void testExceptionsDuringCloseAreThrownAfterAllCursorsHaveBeenClosed() throws Exception {
        doThrow(RuntimeException.class).when(m_cursor).close();
        var firstTrackedCursor = m_tracker.createTrackedCursor(() -> m_cursor);
        @SuppressWarnings("unchecked")
        Cursor<Object> secondCursor = Mockito.mock(Cursor.class);
        var secondTrackedCursor = m_tracker.createTrackedCursor(() -> secondCursor);
        m_tracker.close();
        verify(m_cursor).close();
        verify(secondCursor).close();
    }

    @After
    public void after() {
        ResourceLeakDetector.getInstance().poll();
    }

}