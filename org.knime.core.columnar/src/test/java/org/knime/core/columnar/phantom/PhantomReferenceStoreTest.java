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
 *   20 Aug 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.phantom;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDefaultTable;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDefaultTestColumnStore;
import static org.knime.core.columnar.TestColumnStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.writeTable;

import java.lang.ref.WeakReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.knime.core.columnar.TestColumnStore;
import org.knime.core.columnar.TestColumnStoreUtils.TestTable;
import org.knime.core.columnar.TestDoubleColumnData;
import org.knime.core.columnar.TestDoubleColumnData.Delegate;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class PhantomReferenceStoreTest {

    @Before
    public void setup() {
        CloseableDelegateFinalizer.OPEN_FINALIZERS.clear();
        System.gc();
        CloseableDelegateFinalizer.poll();
    }

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    private static void checkOpenFinalizers(final int numOpenFinalizers) {
        assertEquals(numOpenFinalizers, CloseableDelegateFinalizer.OPEN_FINALIZERS.size());
    }

    private static void checkNoOpenFinalizers() {
        assertTrue("There are enqueued finalizers of delegates.", CloseableDelegateFinalizer.ENQUEUED_FINALIZERS.poll() == null);
        assertTrue("There are open references on finalizers of delegates.", CloseableDelegateFinalizer.OPEN_FINALIZERS.isEmpty());
    }

    @Test
    @SuppressWarnings("resource")
    public void testCloseUnclosedStoreOnGC() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore()) {
            ColumnStore store = PhantomReferenceStore.create(delegate);
            assertFalse(delegate.isStoreClosed());
            checkOpenFinalizers(3); // write store, read store, and writer

            // wait for collection of store
            store = null;
            final WeakReference<Object> ref = new WeakReference<>(store);
            System.gc();
            await().until(() -> ref.get() == null);

            CloseableDelegateFinalizer.poll();
            assertTrue(delegate.isStoreClosed());

            checkNoOpenFinalizers();
        }
    }

    @Test
    @SuppressWarnings("resource")
    public void testCloseUnclosedWriterOnGC() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore()) {
            ColumnStore store = PhantomReferenceStore.create(delegate);
            ColumnDataWriter writer = store.getWriter();
            assertFalse(delegate.isWriterClosed());
            checkOpenFinalizers(3); // write store, read store, and writer

            // wait for collection of writer
            store = null; // store must also be collected, since it holds a reference on the writer
            writer = null;
            final WeakReference<Object> storeRef = new WeakReference<>(store);
            final WeakReference<Object> writerRef = new WeakReference<>(writer);
            System.gc();
            await().until(() -> storeRef.get() == null && writerRef.get() == null);

            CloseableDelegateFinalizer.poll();
            assertTrue(delegate.isWriterClosed());

            checkNoOpenFinalizers();
        }
    }

    @Test
    @SuppressWarnings("resource")
    public void testCloseUnclosedWriterOnCloseStore() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore()) {
            final ColumnStore store = PhantomReferenceStore.create(delegate);
            store.getWriter();
            assertFalse(delegate.isWriterClosed());
            assertEquals(3, CloseableDelegateFinalizer.OPEN_FINALIZERS.size()); // write store, read store, and writer

            store.close();
            assertTrue(delegate.isWriterClosed());

            checkNoOpenFinalizers();
        }
    }

    @Test
    @SuppressWarnings("resource")
    public void testCloseUnclosedReadStoreOnGC() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore()) {
            ColumnReadStore store = PhantomReferenceReadStore.create(delegate);
            assertFalse(delegate.isStoreClosed());
            checkOpenFinalizers(1);

            // wait for collection of store
            store = null;
            final WeakReference<Object> ref = new WeakReference<>(store);
            System.gc();
            await().until(() -> ref.get() == null);

            CloseableDelegateFinalizer.poll();
            assertTrue(delegate.isStoreClosed());

            checkNoOpenFinalizers();
        }
    }

    @Test
    @SuppressWarnings("resource")
    public void testCloseUnclosedReaderOnGC() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnReadStore store = PhantomReferenceReadStore.create(delegate)) {
            delegate.getWriter().close();
            ColumnDataReader reader = store.createReader();
            assertEquals(1, delegate.getNumOpenReaders());
            checkOpenFinalizers(2); // read store and reader

            // wait for collection of reader
            reader = null;
            final WeakReference<Object> ref = new WeakReference<>(reader);
            System.gc();
            await().until(() -> ref.get() == null);

            CloseableDelegateFinalizer.poll();
            assertEquals(0, delegate.getNumOpenReaders());
        }

        checkNoOpenFinalizers();
    }

    @Test
    @SuppressWarnings("resource")
    public void testCloseUnclosedDataOnGC() throws Exception {

        TestDoubleColumnData data = TestDoubleColumnData.create(0d);
        final Delegate dataDelegate = data.getDelegate();
        assertFalse(dataDelegate.isClosed());
        checkOpenFinalizers(1);

        // wait for collection of data
        data = null;
        final WeakReference<Object> ref = new WeakReference<>(data);
        System.gc();
        await().until(() -> ref.get() == null);

        CloseableDelegateFinalizer.poll();
        assertTrue(dataDelegate.isClosed());
        checkNoOpenFinalizers();
    }

    @Test
    public void testWriteRead() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final PhantomReferenceStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {

            writeTable(store, table);
            try (final TestTable reassembledTable = readAndCompareTable(store, table)) {
            }
        }

        checkNoOpenFinalizers();
    }

    @Test
    public void testWriteMultiRead() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final PhantomReferenceStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {

            writeTable(store, table);
            readTwiceAndCompareTable(store);
        }

        checkNoOpenFinalizers();
    }

    @Test
    public void testWriteReadSelection() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final PhantomReferenceStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {

            writeTable(store, table);
            try (final TestTable reassembledTable = readSelectionAndCompareTable(store, table, 0)) {
            }
        }

        checkNoOpenFinalizers();
    }

    @Test
    public void testFactorySingleton() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            assertEquals(store.getFactory(), store.getFactory());
        }

        checkNoOpenFinalizers();
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            store.getFactory();
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            store.close();
            store.getFactory();
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            final ColumnDataFactory factory = store.getFactory();
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            factory.create();
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            final ColumnDataFactory factory = store.getFactory();
            store.close();
            factory.create();
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test
    public void testWriterSingleton() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate);
                final ColumnDataWriter writer1 = store.getWriter();
                final ColumnDataWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }

        checkNoOpenFinalizers();
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            store.close();
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.close();
                writeTable(store, table);
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.close();
                writeTable(store, table);
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.save(null);
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            store.close();
            store.save(null);
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                try (final ColumnDataReader reader = store.createReader()) {
                }
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            store.close();
            try (final ColumnDataReader reader = store.createReader()) {
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            try (final ColumnDataReader reader = store.createReader()) {
                reader.close();
                reader.readRetained(0);
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = PhantomReferenceStore.create(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            try (final ColumnDataReader reader = store.createReader()) {
                store.close();
                reader.readRetained(0);
            }
        } finally {
            checkNoOpenFinalizers();
        }
    }

}
