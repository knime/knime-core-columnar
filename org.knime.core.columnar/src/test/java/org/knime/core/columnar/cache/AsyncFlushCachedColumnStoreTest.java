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
 */
package org.knime.core.columnar.cache;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_HUNDRED_MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.TestColumnStoreUtils.DEF_SIZE_OF_TABLE;
import static org.knime.core.columnar.TestColumnStoreUtils.checkRefs;
import static org.knime.core.columnar.TestColumnStoreUtils.createBatch;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDefaultTable;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDefaultTestColumnStore;
import static org.knime.core.columnar.TestColumnStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.tableInStore;
import static org.knime.core.columnar.TestColumnStoreUtils.writeTable;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.TestDoubleColumnData;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class AsyncFlushCachedColumnStoreTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    private static CachedColumnStoreCache generateCache(final int numTablesHeld) {
        return new CachedColumnStoreCache(numTablesHeld * DEF_SIZE_OF_TABLE);
    }

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    @Test
    public void testWriteReadWait() throws Exception {

        final List<TestDoubleColumnData[]> table = generateDefaultTable();
        assertEquals(1, checkRefs(table));

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store =
                    new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR)) {

            // delay flush
            final CountDownLatch latch = new CountDownLatch(1);
            store.enqueueRunnable(() -> {
                try {
                    latch.await();
                } catch (InterruptedException ex) {
                }
            });

            writeTable(store, table);
            assertEquals(2, checkRefs(table)); // held in the cache
            assertFalse(tableInStore(delegate, table)); // not held in the delegate

            readAndCompareTable(store, table);
            assertEquals(2, checkRefs(table)); // held in the cache
            assertFalse(tableInStore(delegate, table)); // not held in the delegate

            // wait for flush
            latch.countDown();
            store.waitForAndHandleFuture();
            assertEquals(2, checkRefs(table)); // held in the cache
            assertTrue(tableInStore(delegate, table)); // held in the delegate
        }

        assertEquals(1, checkRefs(table));
    }

    @Test
    public void testWriteWaitRead() throws Exception {

        final List<TestDoubleColumnData[]> table = generateDefaultTable();
        assertEquals(1, checkRefs(table));

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store =
                    new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR)) {

            writeTable(store, table);
            assertEquals(2, checkRefs(table)); // held in the cache

            // wait for flush
            store.waitForAndHandleFuture();
            assertEquals(2, checkRefs(table)); // held in the cache
            assertTrue(tableInStore(delegate, table)); // held in the delegate

            readAndCompareTable(store, table);
            assertEquals(2, checkRefs(table)); // held in the cache
            assertTrue(tableInStore(delegate, table)); // held in the delegate
        }

        assertEquals(1, checkRefs(table));
    }

    @Test
    public void testWriteMultiReadWait() throws Exception {

        final List<TestDoubleColumnData[]> smallTable = generateDefaultTable();
        assertEquals(1, checkRefs(smallTable));

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store =
                    new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR)) {

            // delay flush
            final CountDownLatch latch = new CountDownLatch(1);
            store.enqueueRunnable(() -> {
                try {
                    latch.await();
                } catch (InterruptedException ex) {
                }
            });

            writeTable(store, smallTable);
            assertEquals(2, checkRefs(smallTable)); // held in the cache
            assertFalse(tableInStore(delegate, smallTable)); // not held in the delegate

            readTwiceAndCompareTable(store);
            assertEquals(2, checkRefs(smallTable)); // held in the cache
            assertFalse(tableInStore(delegate, smallTable)); // not held in the delegate

            // wait for flush
            latch.countDown();
            store.waitForAndHandleFuture();
            assertEquals(2, checkRefs(smallTable)); // held in the cache
            assertTrue(tableInStore(delegate, smallTable)); // held in the delegate
        }

        assertEquals(1, checkRefs(smallTable));
    }

    @Test
    public void testWriteReadSelectionWait() throws Exception {

        final List<TestDoubleColumnData[]> smallTable = generateDefaultTable();
        assertEquals(1, checkRefs(smallTable));

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store =
                    new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR)) {

            // delay flush
            final CountDownLatch latch = new CountDownLatch(1);
            store.enqueueRunnable(() -> {
                try {
                    latch.await();
                } catch (InterruptedException ex) {
                }
            });

            writeTable(store, smallTable);
            assertEquals(2, checkRefs(smallTable)); // held in the cache
            assertFalse(tableInStore(delegate, smallTable)); // not held in the delegate

            readSelectionAndCompareTable(store, smallTable, 0);
            assertEquals(2, checkRefs(smallTable)); // held in the cache
            assertFalse(tableInStore(delegate, smallTable)); // not held in the delegate

            // wait for flush
            latch.countDown();
            store.waitForAndHandleFuture();
            assertEquals(2, checkRefs(smallTable)); // held in the cache
            assertTrue(tableInStore(delegate, smallTable)); // held in the delegate
        }

        assertEquals(1, checkRefs(smallTable));
    }

    @Test
    public void testWriteWaitWriteEvictWaitRead() throws Exception {

        final List<TestDoubleColumnData[]> table1 = generateDefaultTable();
        final List<TestDoubleColumnData[]> table2 = generateDefaultTable();
        assertEquals(1, checkRefs(table1));
        assertEquals(1, checkRefs(table2));

        final CachedColumnStoreCache cache = generateCache(1); // held by the test

        try (final ColumnStore delegate1 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache, EXECUTOR);
                final ColumnStore delegate2 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store2 =
                    new AsyncFlushCachedColumnStore(delegate2, cache, EXECUTOR)) {

            writeTable(store1, table1);

            // wait for flush of table1
            store1.waitForAndHandleFuture();
            assertEquals(2, checkRefs(table1)); // held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate

            // delay flush of table2
            final CountDownLatch latch = new CountDownLatch(1);
            store2.enqueueRunnable(() -> {
                try {
                    latch.await();
                } catch (InterruptedException ex) {
                }
            });

            writeTable(store2, table2); // evict table1
            assertEquals(1, checkRefs(table1)); // not held in the cache
            assertEquals(2, checkRefs(table2)); // held in the cache

            // wait for flush of table2
            latch.countDown();
            store2.waitForAndHandleFuture();
            assertEquals(1, checkRefs(table1)); // not held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(2, checkRefs(table2)); // held in the cache
            assertTrue(tableInStore(delegate2, table2)); // held in the delegate

            final List<TestDoubleColumnData[]> reassembledTable1 = readAndCompareTable(store1, table1);
            assertEquals(1, checkRefs(table1)); // held in the test, but not in the cache
            assertEquals(1, checkRefs(reassembledTable1)); // held in the cache, but not in the test
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(1, checkRefs(table2)); // not held in the cache
            assertTrue(tableInStore(delegate2, table2)); // held in the delegate
            final List<TestDoubleColumnData[]> reassembledTable2 = readAndCompareTable(store2, table2);
            assertEquals(1, checkRefs(table1)); // not held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(1, checkRefs(table2)); // held in the test, but not in the cache
            assertEquals(1, checkRefs(reassembledTable2)); // held in the cache, but not in the test
            assertTrue(tableInStore(delegate2, table2)); // held in the delegate
        }

        assertEquals(1, checkRefs(table1));
        assertEquals(1, checkRefs(table2));
    }

    @Test
    public void testCacheBlocksOnEvictWhenAllDataUnflushed() throws Exception {

        final List<TestDoubleColumnData[]> table1 = generateDefaultTable();
        final List<TestDoubleColumnData[]> table2 = generateDefaultTable();
        final CachedColumnStoreCache cache = generateCache(1);

        try (final ColumnStore delegate1 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache, EXECUTOR);
                final ColumnStore delegate2 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store2 =
                    new AsyncFlushCachedColumnStore(delegate2, cache, EXECUTOR)) {

            // delay flush of table1
            final CountDownLatch flushLatch = new CountDownLatch(1);
            store1.enqueueRunnable(() -> {
                try {
                    flushLatch.await();
                } catch (InterruptedException ex) {
                }
            });

            writeTable(store1, table1);
            assertEquals(2, checkRefs(table1)); // held in the cache
            assertFalse(tableInStore(delegate1, table1)); // not held in the delegate
            assertEquals(DEF_SIZE_OF_TABLE, cache.size());

            // block on write, since table1 has not been flushed
            final CountDownLatch writeLatch = new CountDownLatch(1);
            final Runnable r = () -> {
                try {
                    writeTable(store2, table2);
                    writeLatch.countDown();
                } catch (Exception e) {
                }
            };
            final Thread t = new Thread(r);
            t.start();
            assertEquals(DEF_SIZE_OF_TABLE, cache.size());
            await().pollDelay(ONE_HUNDRED_MILLISECONDS).until(() -> t.getState() == Thread.State.WAITING);
            assertEquals(DEF_SIZE_OF_TABLE, cache.size());

            // wait for flush of table1
            flushLatch.countDown();
            writeLatch.await();
            store1.waitForAndHandleFuture();

            assertEquals(1, checkRefs(table1)); // not held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(2, checkRefs(table2)); // held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(DEF_SIZE_OF_TABLE, cache.size());
        }
    }

    @Test
    public void testCacheDoesNotBlockOnEvictWhenEvictedDataFlushed() throws Exception {

        final List<TestDoubleColumnData[]> table1 = generateDefaultTable();
        final List<TestDoubleColumnData[]> table2 = generateDefaultTable();
        final List<TestDoubleColumnData[]> table3 = generateDefaultTable();
        final CachedColumnStoreCache cache = generateCache(2);

        try (final ColumnStore delegate1 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache, EXECUTOR);
                final ColumnStore delegate2 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store2 = new AsyncFlushCachedColumnStore(delegate2, cache, EXECUTOR);
                final ColumnStore delegate3 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store3 =
                    new AsyncFlushCachedColumnStore(delegate3, cache, EXECUTOR)) {

            writeTable(store1, table1);
            // wait for flush of table1
            store1.waitForAndHandleFuture();
            assertEquals(2, checkRefs(table1)); // held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(DEF_SIZE_OF_TABLE, cache.size());

            // delay flush of table2 and table3
            final CountDownLatch latch = new CountDownLatch(1);
            store2.enqueueRunnable(() -> {
                try {
                    latch.await();
                } catch (InterruptedException ex) {
                }
            });
            store3.enqueueRunnable(() -> {
                try {
                    latch.await();
                } catch (InterruptedException ex) {
                }
            });

            writeTable(store2, table2); // cache & queue flush of table2
            assertEquals(2, checkRefs(table2)); // held in the cache
            assertFalse(tableInStore(delegate2, table2)); // not held in the delegate
            assertEquals(2 * DEF_SIZE_OF_TABLE, cache.size());
            writeTable(store3, table3); // cache & queue flush of table3, evicting table1
            assertEquals(1, checkRefs(table1)); // not held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(2, checkRefs(table3)); // held in the cache
            assertFalse(tableInStore(delegate3, table3)); // not held in the delegate
            assertEquals(2 * DEF_SIZE_OF_TABLE, cache.size());

            // wait for flush of table2 and table3
            latch.countDown();
            store2.waitForAndHandleFuture();
            store3.waitForAndHandleFuture();

            assertEquals(1, checkRefs(table1)); // not held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(2, checkRefs(table2)); // held in the cache
            assertTrue(tableInStore(delegate2, table2)); // held in the delegate
            assertEquals(2, checkRefs(table3)); // held in the cache
            assertTrue(tableInStore(delegate3, table3)); // held in the delegate
            assertEquals(2 * DEF_SIZE_OF_TABLE, cache.size());
        }
    }

    @Test
    public void testCacheBlocksOnEvictWhenEvictedDataUnflushed() throws Exception {

        final List<TestDoubleColumnData[]> table1 = generateDefaultTable();
        final List<TestDoubleColumnData[]> table2 = generateDefaultTable();
        final List<TestDoubleColumnData[]> table3 = generateDefaultTable();
        final CachedColumnStoreCache cache = generateCache(2);

        try (final ColumnStore delegate1 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache, EXECUTOR);
                final ColumnStore delegate2 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store2 = new AsyncFlushCachedColumnStore(delegate2, cache, EXECUTOR);
                final ColumnStore delegate3 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store3 =
                    new AsyncFlushCachedColumnStore(delegate3, cache, EXECUTOR)) {

            writeTable(store1, table1);
            // wait for flush of table1
            store1.waitForAndHandleFuture();
            assertEquals(2, checkRefs(table1)); // held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(DEF_SIZE_OF_TABLE, cache.size());

            // delay flush of table2 and table3
            final CountDownLatch flushLatch = new CountDownLatch(1);
            store2.enqueueRunnable(() -> {
                try {
                    flushLatch.await();
                } catch (InterruptedException ex) {
                }
            });
            store3.enqueueRunnable(() -> {
                try {
                    flushLatch.await();
                } catch (InterruptedException ex) {
                }
            });

            writeTable(store2, table2); // cache & queue flush of table2
            assertEquals(2, checkRefs(table2)); // held in the cache
            assertFalse(tableInStore(delegate2, table2)); // not held in the delegate
            assertEquals(2 * DEF_SIZE_OF_TABLE, cache.size());
            readAndCompareTable(store1, table1); // read table1 to make table2 next in line for eviction

            // block on write, since table2 has not been flushed
            final CountDownLatch writeLatch = new CountDownLatch(1);
            final Runnable r = () -> {
                try {
                    writeTable(store3, table3);
                    writeLatch.countDown();
                } catch (Exception e) {
                }
            };
            final Thread t = new Thread(r);
            t.start();
            assertEquals(2 * DEF_SIZE_OF_TABLE, cache.size());
            await().pollDelay(ONE_HUNDRED_MILLISECONDS).until(() -> t.getState() == Thread.State.WAITING);
            assertEquals(2 * DEF_SIZE_OF_TABLE, cache.size());

            // wait for flush
            flushLatch.countDown();
            writeLatch.await();
            store2.waitForAndHandleFuture();
            store3.waitForAndHandleFuture();

            assertEquals(2, checkRefs(table1)); // held in the cache
            assertTrue(tableInStore(delegate1, table1)); // held in the delegate
            assertEquals(1, checkRefs(table2)); // not held in the cache
            assertTrue(tableInStore(delegate2, table2)); // held in the delegate
            assertEquals(2, checkRefs(table3)); // held in the cache
            assertTrue(tableInStore(delegate3, table3)); // held in the delegate
            assertEquals(2 * DEF_SIZE_OF_TABLE, cache.size());
        }
    }

    @Test
    public void testCacheEmptyAfterClear() throws Exception {

        final List<TestDoubleColumnData[]> table = generateDefaultTable();
        final CachedColumnStoreCache cache = generateCache(1);

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = new AsyncFlushCachedColumnStore(delegate, cache, EXECUTOR)) {

            writeTable(store, table);
            assertEquals(2, checkRefs(table));

            // wait for flush of table
            store.waitForAndHandleFuture();
            assertEquals(DEF_SIZE_OF_TABLE, cache.size());
            assertEquals(2, checkRefs(table)); // held in the cache
            assertTrue(tableInStore(delegate, table)); // held in the delegate
        }

        assertEquals(0, cache.size());
        assertEquals(1, checkRefs(table));
    }

    @Test
    public void testCacheNotEmptyAfterPartialClear() throws Exception {

        final List<TestDoubleColumnData[]> table1 = generateDefaultTable();
        final List<TestDoubleColumnData[]> table2 = generateDefaultTable();
        final CachedColumnStoreCache cache = generateCache(2);

        try (final ColumnStore delegate1 = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 =
                    new AsyncFlushCachedColumnStore(delegate1, cache, EXECUTOR)) {

            writeTable(store1, table1);

            try (final ColumnStore delegate2 = generateDefaultTestColumnStore();
                    final AsyncFlushCachedColumnStore store2 =
                        new AsyncFlushCachedColumnStore(delegate2, cache, EXECUTOR)) {

                writeTable(store2, table2);

                // wait for flush of tables
                store1.waitForAndHandleFuture();
                store2.waitForAndHandleFuture();
                assertEquals(2 * DEF_SIZE_OF_TABLE, cache.size());
                assertEquals(2, checkRefs(table1)); // held in the cache
                assertTrue(tableInStore(delegate1, table1)); // held in the delegate
                assertEquals(2, checkRefs(table2)); // held in the cache
                assertTrue(tableInStore(delegate2, table1)); // held in the delegate
            }

            assertEquals(DEF_SIZE_OF_TABLE, cache.size());
            assertEquals(2, checkRefs(table1)); // held in the cache
            assertEquals(1, checkRefs(table2)); // not held in the cache
        }

        assertEquals(0, cache.size());
        assertEquals(1, checkRefs(table1)); // not held in the cache
        assertEquals(1, checkRefs(table2)); // not held in the cache
    }

    @Test
    public void testFactorySingleton() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store =
                    new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR)) {
            assertEquals(store.getFactory(), store.getFactory());
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testWriterSingleton() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store =
                    new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR)) {
            assertEquals(store.getWriter(), store.getWriter());
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final AsyncFlushCachedColumnStore store =
                new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            store.close();
            store.getFactory().create();
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final AsyncFlushCachedColumnStore store =
                new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.close();
                writer.write(createBatch(1, 1));
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final AsyncFlushCachedColumnStore store =
                new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            store.close();
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final AsyncFlushCachedColumnStore store =
                new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            try (ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            try (final ColumnDataReader reader = store.createReader()) {
                reader.close();
                reader.read(0);
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final AsyncFlushCachedColumnStore store =
                new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            try (ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            try (final ColumnDataReader reader = store.createReader()) {
                store.close();
                reader.read(0);
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final AsyncFlushCachedColumnStore store =
                new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            store.close();
            store.getFactory();
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final AsyncFlushCachedColumnStore store =
                new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            store.close();
            store.getWriter();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store =
                    new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.saveToFile(null);
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            store.close();
            store.saveToFile(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store =
                    new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                try (final ColumnDataReader reader = store.createReader()) {
                }
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final AsyncFlushCachedColumnStore store =
                new AsyncFlushCachedColumnStore(delegate, generateCache(1), EXECUTOR);
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            store.close();
            try (final ColumnDataReader reader = store.createReader()) {
            }
        }
    }

}
