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
import static org.knime.core.columnar.TestColumnStoreUtils.DEF_SIZE_OF_DATA;
import static org.knime.core.columnar.TestColumnStoreUtils.DEF_SIZE_OF_TABLE;
import static org.knime.core.columnar.TestColumnStoreUtils.checkRefs;
import static org.knime.core.columnar.TestColumnStoreUtils.createDefaultTestColumnStore;
import static org.knime.core.columnar.TestColumnStoreUtils.createDefaultTestTable;
import static org.knime.core.columnar.TestColumnStoreUtils.createEmptyTestTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.tableInStore;
import static org.knime.core.columnar.TestColumnStoreUtils.writeTable;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.knime.core.columnar.TestColumnStoreUtils.TestDataTable;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestColumnStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class AsyncFlushCachedColumnStoreTest extends ColumnarTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

//    @Rule
//    public Timeout globalTimeout = Timeout.seconds(60);

    static CachedColumnStoreCache generateCache(final int numTablesHeld) {
        return new CachedColumnStoreCache(numTablesHeld * DEF_SIZE_OF_TABLE);
    }

    private static AsyncFlushCachedColumnStore generateDefaultCachedColumnStore(final ColumnStore delegate,
        final CachedColumnStoreCache cache) {
        return new AsyncFlushCachedColumnStore(delegate, cache, EXECUTOR);
    }

    private static AsyncFlushCachedColumnStore generateDefaultCachedColumnStore(final ColumnStore delegate) {
        return generateDefaultCachedColumnStore(delegate, generateCache(1));
    }

    private static CountDownLatch delayFlush(final AsyncFlushCachedColumnStore store) {
        final CountDownLatch latch = new CountDownLatch(1);
        store.enqueueRunnable(() -> {
            try {
                latch.await();
            } catch (InterruptedException ex) {
            }
        });
        return latch;
    }

    private static void waitForFlush(final AsyncFlushCachedColumnStore store, final CountDownLatch latch)
        throws InterruptedException {
        latch.countDown();
        store.waitForAndHandleFuture();
    }

    private static CountDownLatch blockOnWriteAfterDelayedFlush(final AsyncFlushCachedColumnStore store,
        final TestDataTable table) {
        final CountDownLatch writeLatch = new CountDownLatch(1);
        final Runnable r = () -> {
            try {
                writeTable(store, table);
                writeLatch.countDown();
            } catch (Exception e) {
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

    static void checkUncached(final TestDataTable table) {
        assertEquals(1, checkRefs(table));
    }

    static void checkCached(final TestDataTable table) {
        assertEquals(2, checkRefs(table));
    }

    private static void checkCacheSize(final CachedColumnStoreCache cache, final int tablesHeldInCache) {
        assertEquals(DEF_SIZE_OF_TABLE * tablesHeldInCache, cache.size() * DEF_SIZE_OF_DATA);
    }

    private static void checkUnflushed(final TestDataTable table, final TestColumnStore delegate) throws IOException {
        assertEquals(3, checkRefs(table));
        assertFalse(tableInStore(delegate, table));
    }

    private static void checkFlushed(final TestDataTable table, final TestColumnStore delegate) throws IOException {
        assertTrue(tableInStore(delegate, table));
    }

    @Test
    public void testWriteReadWait() throws Exception {

        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            final CountDownLatch latch = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);

            try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) {
            }
            checkUnflushed(table, delegate);

            waitForFlush(store, latch);
            checkCached(table);
            checkFlushed(table, delegate);
        }
    }

    @Test
    public void testWriteWaitRead() throws Exception {

        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            final CountDownLatch latch = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);

            waitForFlush(store, latch);
            checkCached(table);
            checkFlushed(table, delegate);

            try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) {
            }
            checkCached(table);
            checkFlushed(table, delegate);
        }
    }

    @Test
    public void testWriteMultiReadWait() throws Exception {

        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            final CountDownLatch latch = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);

            readTwiceAndCompareTable(store);
            checkUnflushed(table, delegate);

            waitForFlush(store, latch);
            checkCached(table);
            checkFlushed(table, delegate);
        }

    }

    @Test
    public void testWriteReadSelectionWait() throws Exception {

        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            final CountDownLatch latch = delayFlush(store);
            writeTable(store, table);
            checkUnflushed(table, delegate);

            try (final TestDataTable reassembledTable = readSelectionAndCompareTable(store, table, 0)) {
            }
            checkUnflushed(table, delegate);

            waitForFlush(store, latch);
            checkCached(table);
            checkFlushed(table, delegate);
        }
    }

    @Test
    public void testWriteWaitWriteEvictWaitRead() throws Exception {

        final CachedColumnStoreCache cache = generateCache(1);
        try (final TestColumnStore delegate1 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestColumnStore delegate2 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store2 = generateDefaultCachedColumnStore(delegate2, cache);
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

    @Test
    public void testCacheBlocksOnEvictWhenAllDataUnflushed() throws Exception {

        final CachedColumnStoreCache cache = generateCache(1);
        try (final TestColumnStore delegate1 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestColumnStore delegate2 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store2 = generateDefaultCachedColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate2)) {

            final CountDownLatch flushLatch1 = delayFlush(store1);
            writeTable(store1, table1);
            checkUnflushed(table1, delegate1);
            checkCacheSize(cache, 1);

            // block on write, since table1 has not been flushed
            checkCacheSize(cache, 1);
            final CountDownLatch writeLatch2 = blockOnWriteAfterDelayedFlush(store2, table2);
            checkCacheSize(cache, 1);

            // wait for flush of table1
            waitForFlush(store1, flushLatch1);
            waitForWrite(writeLatch2);

            assertEquals(1, checkRefs(table1)); // not held in the cache
            checkFlushed(table1, delegate1);
            checkCached(table2);
            checkFlushed(table2, delegate2);
            checkCacheSize(cache, 1);
        }
    }

    @Test
    public void testCacheDoesNotBlockOnEvictWhenEvictedDataFlushed() throws Exception {

        final CachedColumnStoreCache cache = generateCache(2);
        try (final TestColumnStore delegate1 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestColumnStore delegate2 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store2 = generateDefaultCachedColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate1);
                final TestColumnStore delegate3 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store3 = generateDefaultCachedColumnStore(delegate3, cache);
                final TestDataTable table3 = createDefaultTestTable(delegate2)) {

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
    public void testCacheBlocksOnEvictWhenEvictedDataUnflushed() throws Exception {

        final CachedColumnStoreCache cache = generateCache(2);
        try (final TestColumnStore delegate1 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestColumnStore delegate2 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store2 = generateDefaultCachedColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate1);
                final TestColumnStore delegate3 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store3 = generateDefaultCachedColumnStore(delegate3, cache);
                final TestDataTable table3 = createDefaultTestTable(delegate2)) {

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
            try (final TestDataTable reassembledTable1 = readAndCompareTable(store1, table1)) {
                // read table1 to make table2 next in line for eviction
            }

            // block on write, since table2 has not been flushed
            final CountDownLatch writeLatch3 = blockOnWriteAfterDelayedFlush(store3, table3);

            waitForFlush(store2, flushLatch2);
            waitForFlush(store3, flushLatch3);
            waitForWrite(writeLatch3);

            checkCached(table1);
            checkFlushed(table1, delegate1);
            checkUncached(table2);
            checkFlushed(table2, delegate2);
            checkCached(table3);
            checkFlushed(table3, delegate3);
            checkCacheSize(cache, 2);
        }
    }

    @Test
    public void testCacheEmptyAfterClear() throws Exception {

        final CachedColumnStoreCache cache = generateCache(1);
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate, cache);
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
    public void testCacheNotEmptyAfterPartialClear() throws Exception {

        final CachedColumnStoreCache cache = generateCache(2);
        try (final TestColumnStore delegate1 = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store1 = generateDefaultCachedColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1)) {

            writeTable(store1, table1);

            try (final TestColumnStore delegate2 = createDefaultTestColumnStore();
                    final AsyncFlushCachedColumnStore store2 = generateDefaultCachedColumnStore(delegate2, cache);
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
    public void testFactorySingleton() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            assertEquals(store.getFactory(), store.getFactory());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterWriterClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            store.getFactory();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            store.close();
            store.getFactory();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterWriterClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            final ColumnDataFactory factory = store.getFactory();
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            factory.create(DEF_SIZE_OF_DATA);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            final ColumnDataFactory factory = store.getFactory();
            store.close();
            factory.create(DEF_SIZE_OF_DATA);
        }
    }

    @Test
    public void testWriterSingleton() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final ColumnDataWriter writer1 = store.getWriter();
                final ColumnDataWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterWriterClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            store.close();
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.close();
                writeTable(store, table);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createEmptyTestTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.close();
                writeTable(store, table);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.save(null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createEmptyTestTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            store.close();
            store.save(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                try (final ColumnDataReader reader = store.createReader()) {
                }
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createEmptyTestTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            store.close();
            try (final ColumnDataReader reader = store.createReader()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {
            try (ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            try (final ColumnDataReader reader = store.createReader()) {
                reader.close();
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws Exception {
        try (final TestColumnStore delegate = createDefaultTestColumnStore();
                final AsyncFlushCachedColumnStore store = generateDefaultCachedColumnStore(delegate);
                final TestDataTable table = createEmptyTestTable(delegate)) {
            try (ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            try (final ColumnDataReader reader = store.createReader()) {
                store.close();
                reader.readRetained(0);
            }
        }
    }

}
