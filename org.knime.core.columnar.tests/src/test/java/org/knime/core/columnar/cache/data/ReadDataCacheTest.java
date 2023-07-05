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
package org.knime.core.columnar.cache.data;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_HUNDRED_MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.TestBatchStoreUtils.DEF_NUM_BATCHES;
import static org.knime.core.columnar.TestBatchStoreUtils.DEF_NUM_COLUMNS;
import static org.knime.core.columnar.TestBatchStoreUtils.DEF_SIZE_OF_TABLE;
import static org.knime.core.columnar.TestBatchStoreUtils.checkRefs;
import static org.knime.core.columnar.TestBatchStoreUtils.createDefaultTestColumnStore;
import static org.knime.core.columnar.TestBatchStoreUtils.createDefaultTestTable;
import static org.knime.core.columnar.TestBatchStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.tableInStore;
import static org.knime.core.columnar.TestBatchStoreUtils.writeTable;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.knime.core.columnar.TestBatchStoreUtils.TestDataTable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestBatchStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ReadDataCacheTest extends ColumnarTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    @AfterClass
    public static void tearDownTests() {
        EXECUTOR.shutdown();
    }

    static SharedReadDataCache generateCache(final int numTablesHeld) {
        return new SharedReadDataCache((long)numTablesHeld * DEF_SIZE_OF_TABLE, 1);
    }

    private static ReadDataCache generateDefaultCachedColumnStore(final BatchStore delegate,
        final SharedReadDataCache cache) {
        return new ReadDataCache(delegate, delegate, cache, EXECUTOR);
    }

    private static ReadDataCache generateDefaultCachedColumnStore(final BatchStore delegate) {
        return generateDefaultCachedColumnStore(delegate, generateCache(1));
    }

    private static CountDownLatch delayFlush(final ReadDataCache store) {
        final CountDownLatch latch = new CountDownLatch(1);
        store.enqueueRunnable(() -> {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
            }
        });
        return latch;
    }

    private static void waitForFlush(final ReadDataCache store, final CountDownLatch latch)
        throws InterruptedException {
        latch.countDown();
        store.waitForAndHandleFuture();
    }

    private static CountDownLatch blockOnWriteAfterDelayedFlush(final ReadDataCache store, final TestDataTable table) {
        final CountDownLatch writeLatch = new CountDownLatch(1);
        final Runnable r = () -> {
            try {
                writeTable(store, table);
                writeLatch.countDown();
            } catch (IOException e) { // NOSONAR
                Assert.fail();
            }
        };
        final Thread t = new Thread(r);
        t.start();
        await().pollDelay(ONE_HUNDRED_MILLISECONDS).until(() -> t.getState() == Thread.State.WAITING);
        return writeLatch;
    }

    private static void waitForWrite(final CountDownLatch latch) throws InterruptedException {
        latch.await();
    }

    static void readConcurrently(final int numConcurrentLoads, final RandomAccessBatchReadable store,
        final TestDataTable table) throws InterruptedException {
        final ExecutorService threadPool = Executors.newFixedThreadPool(numConcurrentLoads);
        for (int i = 0; i < numConcurrentLoads; i++) {
            threadPool.submit(() -> {
                try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) { // NOSONAR
                }
                return null;
            });
        }
        threadPool.shutdown();
        if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
            threadPool.shutdownNow();
        }
    }

    static void checkUncached(final TestDataTable table) {
        assertEquals(1, checkRefs(table));
    }

    static void checkCached(final TestDataTable table) {
        assertEquals(2, checkRefs(table));
    }

    private static void checkCacheSize(final SharedReadDataCache cache, final int tablesHeldInCache) {
        assertEquals((long)DEF_NUM_BATCHES * DEF_NUM_COLUMNS * tablesHeldInCache, cache.size());
    }

    private static void checkUnflushed(final TestDataTable table, final TestBatchStore delegate) throws IOException {
        // refs are the TestDataTable, the unflushed batch and the cache
        assertEquals(3, checkRefs(table));
        assertFalse(tableInStore(delegate, table));
    }

    private static void checkFlushed(final TestDataTable table, final TestBatchStore delegate) throws IOException {
        assertTrue(tableInStore(delegate, table));
    }

    @Test
    public void testWriteReadWait() throws IOException, InterruptedException {

        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final ReadDataCache store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            final CountDownLatch latch = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);

            try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) { // NOSONAR
            }
            checkUnflushed(table, delegate);

            waitForFlush(store, latch);
            checkCached(table);
            checkFlushed(table, delegate);
        }
    }

    @Test
    public void testWriteWaitRead() throws IOException, InterruptedException {

        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final ReadDataCache store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            final CountDownLatch latch = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);

            waitForFlush(store, latch);
            checkCached(table);
            checkFlushed(table, delegate);

            try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) { // NOSONAR
            }
            checkCached(table);
            checkFlushed(table, delegate);
        }
    }

    @Test
    public void testWriteMultiReadWait() throws IOException, InterruptedException {

        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final ReadDataCache store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            final CountDownLatch latch = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);

            readTwiceAndCompareTable(store, table.size());
            checkUnflushed(table, delegate);

            waitForFlush(store, latch);
            checkCached(table);
            checkFlushed(table, delegate);
        }

    }

    @Test
    public void testWriteReadSelectionWait() throws IOException, InterruptedException {

        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final ReadDataCache store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            final CountDownLatch latch = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);

            try (final TestDataTable reassembledTable = readSelectionAndCompareTable(store, table, 0)) { // NOSONAR
            }
            checkUnflushed(table, delegate);

            waitForFlush(store, latch);
            checkCached(table);
            checkFlushed(table, delegate);
        }
    }

    @Test
    public void testWriteWaitWriteEvictWaitRead() throws IOException, InterruptedException {

        final SharedReadDataCache cache = generateCache(1);
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final ReadDataCache store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestBatchStore delegate2 = createDefaultTestColumnStore();
                final ReadDataCache store2 = generateDefaultCachedColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate2)) {

            final CountDownLatch latch1 = delayFlush(store1);
            writeTable(store1, table1);
            checkUnflushed(table1, delegate1);

            waitForFlush(store1, latch1);
            checkCached(table1);
            checkFlushed(table1, delegate1);

            final CountDownLatch latch2 = delayFlush(store2);
            writeTable(store2, table2); // evict table1
            checkUncached(table1);

            waitForFlush(store2, latch2);
            checkUncached(table1);
            checkFlushed(table1, delegate1);
            checkCached(table2);
            checkFlushed(table2, delegate2);

            try (final TestDataTable reassembledTable1 = readAndCompareTable(store1, table1);) {
                checkUncached(table1);
                checkFlushed(table1, delegate1);
                checkCached(reassembledTable1);
                checkFlushed(reassembledTable1, delegate1);
                checkUncached(table2);
                checkFlushed(table2, delegate2);
            }

            try (final TestDataTable reassembledTable2 = readAndCompareTable(store2, table2);) {
                checkUncached(table1);
                checkFlushed(table1, delegate1);
                checkUncached(table2);
                checkCached(reassembledTable2);
                checkFlushed(table2, delegate2);
            }
        }
    }

    @Test(timeout = 1000)
    public void testWritingAfterDataReleasedFromCache() throws IOException, InterruptedException {
        final SharedReadDataCache cache = generateCache(1);
        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final ReadDataCache store = generateDefaultCachedColumnStore(delegate, cache);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            // Start writing a table (blocked by flushLatch1)
            final CountDownLatch flushLatch1 = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);
            checkCacheSize(cache, 1);

            // Clear the cache - should work even if the write is still blocked
            cache.getCache().invalidateAll();
            checkCacheSize(cache, 0);

            // Flushing the table should still be blocked
            assertFalse(tableInStore(delegate, table));
            assertEquals(2, checkRefs(table)); // refs: TestDataTable + store | not: cache

            // Finally flush the batch - this should still work
            waitForFlush(store, flushLatch1);
            assertEquals(1, checkRefs(table)); // refs: TestDataTable | not: cache + store

            // The cache should still be empty and the table flushed
            checkCacheSize(cache, 0);
            checkFlushed(table, delegate);
        }
    }

    @Test(timeout = 1000)
    public void testCacheDoesNotBlockOnEvictWhenEvictedDataFlushed() throws IOException, InterruptedException {

        final SharedReadDataCache cache = generateCache(2);
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final ReadDataCache store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestBatchStore delegate2 = createDefaultTestColumnStore();
                final ReadDataCache store2 = generateDefaultCachedColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate2);
                final TestBatchStore delegate3 = createDefaultTestColumnStore();
                final ReadDataCache store3 = generateDefaultCachedColumnStore(delegate3, cache);
                final TestDataTable table3 = createDefaultTestTable(delegate3)) {

            writeTable(store1, table1);
            store1.waitForAndHandleFuture(); // wait for flush of table1
            checkCached(table1);
            checkFlushed(table1, delegate1);
            checkCacheSize(cache, 1);

            final CountDownLatch flushLatch2 = delayFlush(store2);
            final CountDownLatch flushLatch3 = delayFlush(store3);

            writeTable(store2, table2); // cache & queue flush of table2
            checkUnflushed(table2, delegate2);
            checkCacheSize(cache, 2);

            writeTable(store3, table3); // cache & queue flush of table3, evicting table1
            checkUncached(table1);
            checkFlushed(table1, delegate1);
            checkUnflushed(table3, delegate3);
            checkCacheSize(cache, 2);

            waitForFlush(store2, flushLatch2);
            waitForFlush(store3, flushLatch3);

            checkUncached(table1);
            checkFlushed(table1, delegate1);
            checkCached(table2);
            checkFlushed(table2, delegate2);
            checkCached(table3);
            checkFlushed(table3, delegate3);
            checkCacheSize(cache, 2);
        }
    }

    @Test
    public void testCacheEmptyAfterClear() throws IOException, InterruptedException {

        final SharedReadDataCache cache = generateCache(1);
        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final ReadDataCache store = generateDefaultCachedColumnStore(delegate, cache);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            writeTable(store, table);
            store.waitForAndHandleFuture(); // wait for flush of table
            checkCacheSize(cache, 1);
            checkCached(table);
            checkFlushed(table, delegate);
        }

        checkCacheSize(cache, 0);
    }

    @Test
    public void testCacheNotEmptyAfterPartialClear() throws IOException, InterruptedException {

        final SharedReadDataCache cache = generateCache(2);
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final ReadDataCache store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1)) {

            writeTable(store1, table1);

            try (final TestBatchStore delegate2 = createDefaultTestColumnStore();
                    final ReadDataCache store2 = generateDefaultCachedColumnStore(delegate2, cache);
                    final TestDataTable table2 = createDefaultTestTable(delegate2)) {

                writeTable(store2, table2);
                // wait for flush of tables
                store1.waitForAndHandleFuture();
                store2.waitForAndHandleFuture();
                checkCacheSize(cache, 2);
                checkCached(table1);
                checkFlushed(table1, delegate1);
                checkCached(table2);
                checkFlushed(table2, delegate2);
            }

            checkCacheSize(cache, 1);
            checkCached(table1);
        }

        checkCacheSize(cache, 0);
    }

    @Test
    public void testMultiCacheMisses() throws IOException, InterruptedException {
        final SharedReadDataCache cache = generateCache(1);
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final ReadDataCache store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestBatchStore delegate2 = createDefaultTestColumnStore();
                final ReadDataCache store2 = generateDefaultCachedColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate2)) {

            final CountDownLatch latch1 = delayFlush(store1);
            writeTable(store1, table1);
            waitForFlush(store1, latch1);
            final CountDownLatch latch2 = delayFlush(store2);
            writeTable(store2, table2); // evict table1
            waitForFlush(store2, latch2);

            readConcurrently(4, store1, table1);
        }
    }

    @Test
    public void testWriterSingleton() throws IOException {
        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final ReadDataCache store = generateDefaultCachedColumnStore(delegate);
                final BatchWriter writer1 = store.getWriter();
                final BatchWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }
    }

    @Test(timeout = 2000)
    public void testNoDeadlockOnCloseWhileWriting() throws IOException, InterruptedException {

        final SharedReadDataCache cache = generateCache(1);
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final ReadDataCache store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1)) {

            final CountDownLatch latch1 = delayFlush(store1);
            writeTable(store1, table1);
            latch1.countDown();
            // Close and flush are waiting for each other and can lead to a deadlock.
            // Thus we end this test after 2 seconds with a timeout because that means we're stuck.
        }

        checkCacheSize(cache, 0);
    }
}
