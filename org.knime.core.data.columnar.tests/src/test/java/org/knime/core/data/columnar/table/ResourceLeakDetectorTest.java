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
 *   26 Oct 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.knime.core.data.columnar.table.ColumnarTableTestUtils.createUnsavedColumnarContainerTable;

import java.lang.ref.WeakReference;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.node.ExtensionTable;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ResourceLeakDetectorTest extends ColumnarTest {

    private final int m_chunkSize = ColumnarTableTestUtils.getChunkSize();

    @Override
    @Before
    public void setup() {
        ResourceLeakDetector.getInstance().clear();
        System.gc(); // NOSONAR
        ResourceLeakDetector.getInstance().poll();
    }

    @Override
    @After
    public void tearDown() {
        ResourceLeakDetector.getInstance().poll();
        checkNoOpenFinalizers();
    }

    @Rule
    public Timeout m_globalTimeout = Timeout.seconds(120);

    private static void checkOpenFinalizers(final int numOpenFinalizers) {
        ResourceLeakDetector.getInstance().poll();
        assertEquals(numOpenFinalizers, ResourceLeakDetector.getInstance().getNumOpenFinalizers());
    }

    private static void checkNoOpenFinalizers() {
        ResourceLeakDetector.getInstance().poll();
        assertTrue("There are open references on finalizers of delegates.",
            ResourceLeakDetector.getInstance().getNumOpenFinalizers() == 0);
    }

    private static void testCloseUnclosedCloseableOnGC(final Supplier<AutoCloseable> supplier) {
        final int numOpenFinalizers = ResourceLeakDetector.getInstance().getNumOpenFinalizers();
        @SuppressWarnings("resource")
        AutoCloseable closeable = supplier.get(); // NOSONAR
        checkOpenFinalizers(numOpenFinalizers + 1);

        closeable = null;
        final WeakReference<AutoCloseable> ref = new WeakReference<>(closeable);
        System.gc(); // NOSONAR
        await().until(() -> ref.get() == null);

        checkOpenFinalizers(numOpenFinalizers);
    }

    @Test
    public void testColumnarRowContainerReleaseResourcesOnGC() {
        testCloseUnclosedCloseableOnGC(ColumnarTableTestUtils::createColumnarRowContainer);
    }

    @Test
    public void testUnsavedColumnarContainerTableReleaseResourcesOnGC() {
        testCloseUnclosedCloseableOnGC(() -> createUnsavedColumnarContainerTable(0));
    }

    // TODO
    //    public void testSavedColumnarContainerTableReleaseResourcesOnGC() {
    //
    //    }

    @Test
    public void testCloseUnclosedEmptyIteratorOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0)) {
            testCloseUnclosedCloseableOnGC(table::iterator);
        }
    }

    @Test
    public void testCloseUnclosedShortIteratorOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(1)) {
            testCloseUnclosedCloseableOnGC(table::iterator);
        }
    }

    @Test
    public void testCloseUnclosedLongIteratorOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(m_chunkSize + 1)) {
            testCloseUnclosedCloseableOnGC(table::iterator);
        }
    }

    @Test
    public void testCloseUnclosedEmptyIteratorWithFilterOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0)) {
            testCloseUnclosedCloseableOnGC(() -> table.iteratorWithFilter(new TableFilter.Builder().build()));
        }
    }

    @Test
    public void testCloseUnclosedShortIteratorWithFilterOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(1)) {
            testCloseUnclosedCloseableOnGC(() -> table.iteratorWithFilter(new TableFilter.Builder().build()));
        }
    }

    @Test
    public void testCloseUnclosedLongIteratorWithFilterOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(m_chunkSize + 1)) {
            testCloseUnclosedCloseableOnGC(() -> table.iteratorWithFilter(new TableFilter.Builder().build()));
        }
    }

    @Test
    public void testCloseUnclosedEmptyCursorOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0)) {
            testCloseUnclosedCloseableOnGC(table::cursor);
        }
    }

    @Test
    public void testCloseUnclosedShortCursorOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(1)) {
            testCloseUnclosedCloseableOnGC(table::cursor);
        }
    }

    @Test
    public void testCloseUnclosedLongCursorOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(m_chunkSize + 1)) {
            testCloseUnclosedCloseableOnGC(table::cursor);
        }
    }

    @Test
    public void testCloseUnclosedEmptyCursorWithFilterOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0)) {
            testCloseUnclosedCloseableOnGC(() -> table.cursor(new TableFilter.Builder().build()));
        }
    }

    @Test
    public void testCloseUnclosedShortCursorWithFilterOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(1)) {
            testCloseUnclosedCloseableOnGC(() -> table.cursor(new TableFilter.Builder().build()));
        }
    }

    @Test
    public void testCloseUnclosedLongCursorWithFilterOnGC() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(m_chunkSize + 1)) {
            testCloseUnclosedCloseableOnGC(() -> table.cursor(new TableFilter.Builder().build()));
        }
    }

    @Test
    public void testCloseUnclosedEmptyIteratorOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0);
                final CloseableRowIterator iterator = table.iterator()) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedShortIteratorOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(1);
                final CloseableRowIterator iterator = table.iterator()) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedLongIteratorOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(m_chunkSize + 1);
                final CloseableRowIterator iterator = table.iterator()) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedEmptyIteratorWithFilterOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0);
                final CloseableRowIterator iterator = table.iteratorWithFilter(new TableFilter.Builder().build())) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedShortIteratorWithFilterOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(1);
                final CloseableRowIterator iterator = table.iteratorWithFilter(new TableFilter.Builder().build())) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedLongIteratorWithFilterOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(m_chunkSize + 1);
                final CloseableRowIterator iterator = table.iteratorWithFilter(new TableFilter.Builder().build())) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedEmptyCursorOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0);
                final RowCursor cursor = table.cursor()) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedShortCursorOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(1);
                final RowCursor cursor = table.cursor()) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedLongCursorOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(m_chunkSize + 1);
                final RowCursor cursor = table.cursor()) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedEmptyCursorWithFilterOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(0);
                final RowCursor cursor = table.cursor(new TableFilter.Builder().build())) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedShortCursorWithFilterOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(1);
                final RowCursor cursor = table.cursor(new TableFilter.Builder().build())) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testCloseUnclosedLongCursorWithFilterOnTableClear() {
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(m_chunkSize + 1);
                final RowCursor cursor = table.cursor(new TableFilter.Builder().build())) {
            table.clear();
            checkNoOpenFinalizers();
        }
    }

}
