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
import static org.knime.core.columnar.cache.CacheTestUtils.checkRefs;
import static org.knime.core.columnar.cache.CacheTestUtils.createBatch;
import static org.knime.core.columnar.cache.CacheTestUtils.createSchema;
import static org.knime.core.columnar.cache.CacheTestUtils.createTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.writeTable;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.CacheTestUtils.TestColumnData;
import org.knime.core.columnar.cache.CachedColumnReadStore.CachedColumnReadStoreCache;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class AsyncFlushCachedColumnStoreTest {

    private static final int TABLE_HEIGHT = 2;

    private static final int TABLE_WIDTH = 2;

    private static final int SIZE_OF_COLUMN_DATA = 1;

    private static final int SIZE_OF_TABLE = TABLE_HEIGHT * TABLE_WIDTH * SIZE_OF_COLUMN_DATA;

    private static CachedColumnReadStoreCache generateCache(final int numTablesHeld) {
        return new CachedColumnReadStoreCache(numTablesHeld * SIZE_OF_TABLE);
    }

    private static List<TestColumnData[]> generateTable() {
        return createTable(TABLE_HEIGHT, TABLE_WIDTH, SIZE_OF_COLUMN_DATA);
    }

    private static ColumnStoreSchema generateSchema() {
        return createSchema(TABLE_WIDTH);
    }

    @Test(timeout = 60_000)
    public void testWriteReadWait() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        assertEquals(0, checkRefs(table));

        // delay flush
        final Lock lock = new ReentrantLock();
        lock.lock();
        AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            lock.lock();
        });

        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1))) {

            writeTable(store, table);
            assertEquals(1, checkRefs(table));

            readAndCompareTable(store, table);
            assertEquals(1, checkRefs(table));

            // wait for flush
            lock.unlock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();
            assertEquals(2, checkRefs(table)); // held once in the delegate and once in the cache
        }

        assertEquals(0, checkRefs(table));
    }

    @Test(timeout = 60_000)
    public void testWriteWaitRead() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        assertEquals(0, checkRefs(table));

        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final AsyncFlushCachedColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1))) {

            writeTable(store, table);

            // wait for flush
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();
            assertEquals(2, checkRefs(table)); // held once in the delegate and once in the cache

            readAndCompareTable(store, table);
            assertEquals(2, checkRefs(table));
        }

        assertEquals(0, checkRefs(table));
    }

    @Test(timeout = 60_000)
    public void testWriteMultiReadWait() throws Exception {

        final List<TestColumnData[]> smallTable = generateTable();
        assertEquals(0, checkRefs(smallTable));

        // delay flush
        final Lock lock = new ReentrantLock();
        lock.lock();
        AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            lock.lock();
        });

        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1))) {

            writeTable(store, smallTable);
            assertEquals(1, checkRefs(smallTable));

            readTwiceAndCompareTable(store);
            assertEquals(1, checkRefs(smallTable));

            // wait for flush
            lock.unlock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();
            assertEquals(2, checkRefs(smallTable)); // held once in the delegate and once in the cache
        }

        assertEquals(0, checkRefs(smallTable));
    }

    @Test(timeout = 60_000)
    public void testWriteReadSelectionWait() throws Exception {

        final List<TestColumnData[]> smallTable = generateTable();
        assertEquals(0, checkRefs(smallTable));

        // delay flush
        final Lock lock = new ReentrantLock();
        lock.lock();
        AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            lock.lock();
        });

        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1))) {

            writeTable(store, smallTable);
            assertEquals(1, checkRefs(smallTable));

            readSelectionAndCompareTable(store, smallTable, 0);
            assertEquals(1, checkRefs(smallTable));

            // wait for flush
            lock.unlock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();
            assertEquals(2, checkRefs(smallTable)); // held once in the delegate and once in the cache
        }

        assertEquals(0, checkRefs(smallTable));
    }

    @Test(timeout = 60_000)
    public void testWriteWaitWriteEvictWaitRead() throws Exception {

        final List<TestColumnData[]> table1 = generateTable();
        final List<TestColumnData[]> table2 = generateTable();
        assertEquals(0, checkRefs(table1));
        assertEquals(0, checkRefs(table2));

        final ColumnStoreSchema schema = generateSchema();
        final CachedColumnReadStoreCache cache = generateCache(1);

        try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
                final ColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache);
                final ColumnStore delegate2 = new InMemoryColumnStore(schema);
                final ColumnStore store2 = new AsyncFlushCachedColumnStore(delegate2, cache)) {

            writeTable(store1, table1);

            // wait for flush of table1
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();
            assertEquals(2, checkRefs(table1));

            // delay flush of tables written from here on out
            final Lock lock = new ReentrantLock();
            lock.lock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
                lock.lock();
            });

            writeTable(store2, table2); // evict table1
            assertEquals(1, checkRefs(table1)); // held in the delegate
            assertEquals(1, checkRefs(table2)); // held in the cache

            // wait for flush of table2
            lock.unlock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();
            assertEquals(1, checkRefs(table1)); // held in the delegate
            assertEquals(2, checkRefs(table2)); // held once in the delegate and once in the cache

            readAndCompareTable(store1, table1);
            assertEquals(2, checkRefs(table1)); // held once in the delegate and once in the cache
            assertEquals(1, checkRefs(table2)); // held in the delegate
            readAndCompareTable(store2, table2);
            assertEquals(1, checkRefs(table1)); // held in the delegate
            assertEquals(2, checkRefs(table2)); // held once in the delegate and once in the cache
        }

        assertEquals(0, checkRefs(table1));
        assertEquals(0, checkRefs(table2));
    }

    @Test(timeout = 60_000)
    public void testCacheBlocksOnEvictWhenAllDataUnflushed() throws Exception {

        final List<TestColumnData[]> table1 = generateTable();
        final List<TestColumnData[]> table2 = generateTable();
        final ColumnStoreSchema schema = generateSchema();
        final CachedColumnReadStoreCache cache = generateCache(1);

        // delay flush
        final Lock flushLock = new ReentrantLock();
        flushLock.lock();
        AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            flushLock.lock();
        });

        try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
                final ColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache);
                final ColumnStore delegate2 = new InMemoryColumnStore(schema);
                final ColumnStore store2 = new AsyncFlushCachedColumnStore(delegate2, cache)) {

            writeTable(store1, table1);
            assertEquals(1, checkRefs(table1)); // held in the cache
            assertEquals(SIZE_OF_TABLE, cache.size());

            // block on write, since table1 has not been flushed
            final Lock writeLock = new ReentrantLock();
            writeLock.lock();
            final Runnable r = () -> {
                try {
                    writeTable(store2, table2);
                    writeLock.unlock();
                } catch (Exception e) {
                }
            };
            final Thread t = new Thread(r);
            t.start();
            assertEquals(SIZE_OF_TABLE, cache.size());
            await().pollDelay(ONE_HUNDRED_MILLISECONDS).until(() -> t.getState() == Thread.State.WAITING);
            assertEquals(SIZE_OF_TABLE, cache.size());

            // wait for flush
            flushLock.unlock();
            writeLock.lock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();

            assertEquals(1, checkRefs(table1)); // held in the delegate
            assertEquals(2, checkRefs(table2)); // held once in the delegate and once in the cache
            assertEquals(SIZE_OF_TABLE, cache.size());
        }
    }

    @Test(timeout = 60_000)
    public void testCacheDoesNotBlockOnEvictWhenEvictedDataFlushed() throws Exception {

        final List<TestColumnData[]> table1 = generateTable();
        final List<TestColumnData[]> table2 = generateTable();
        final List<TestColumnData[]> table3 = generateTable();
        final ColumnStoreSchema schema = generateSchema();
        final CachedColumnReadStoreCache cache = generateCache(2);

        try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
                final ColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache);
                final ColumnStore delegate2 = new InMemoryColumnStore(schema);
                final ColumnStore store2 = new AsyncFlushCachedColumnStore(delegate2, cache);
                final ColumnStore delegate3 = new InMemoryColumnStore(schema);
                final ColumnStore store3 = new AsyncFlushCachedColumnStore(delegate3, cache)) {

            writeTable(store1, table1); // cache & flush table1
            assertEquals(SIZE_OF_TABLE, cache.size());

            // delay flush
            final Lock lock = new ReentrantLock();
            lock.lock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
                lock.lock();
            });

            writeTable(store2, table2); // cache & queue flush of table2
            assertEquals(1, checkRefs(table2)); // held in the cache
            assertEquals(2 * SIZE_OF_TABLE, cache.size());
            writeTable(store3, table3); // cache & queue flush of table3, evicting table1
            assertEquals(1, checkRefs(table1)); // held in the delegate
            assertEquals(1, checkRefs(table3)); // held in the cache
            assertEquals(2 * SIZE_OF_TABLE, cache.size());

            // wait for flush
            lock.unlock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();

            assertEquals(1, checkRefs(table1)); // held in the delegate
            assertEquals(2, checkRefs(table2)); // held once in the delegate and once in the cache
            assertEquals(2, checkRefs(table3)); // held once in the delegate and once in the cache
            assertEquals(2 * SIZE_OF_TABLE, cache.size());
        }
    }

    @Test(timeout = 60_000)
    public void testCacheBlocksOnEvictWhenEvictedDataUnflushed() throws Exception {

        final List<TestColumnData[]> table1 = generateTable();
        final List<TestColumnData[]> table2 = generateTable();
        final List<TestColumnData[]> table3 = generateTable();
        final ColumnStoreSchema schema = generateSchema();
        final CachedColumnReadStoreCache cache = generateCache(2);

        try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
                final ColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache);
                final ColumnStore delegate2 = new InMemoryColumnStore(schema);
                final ColumnStore store2 = new AsyncFlushCachedColumnStore(delegate2, cache);
                final ColumnStore delegate3 = new InMemoryColumnStore(schema);
                final ColumnStore store3 = new AsyncFlushCachedColumnStore(delegate3, cache)) {

            writeTable(store1, table1); // cache & flush table1
            assertEquals(SIZE_OF_TABLE, cache.size());

            // delay flush
            final Lock flushLock = new ReentrantLock();
            flushLock.lock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
                flushLock.lock();
            });

            writeTable(store2, table2); // cache & queue flush of table2
            assertEquals(1, checkRefs(table2)); // held in the cache
            assertEquals(2 * SIZE_OF_TABLE, cache.size());
            readAndCompareTable(store1, table1); // read table1 to make table2 next in line for eviction

            // block on write, since table2 has not been flushed
            final Lock writeLock = new ReentrantLock();
            writeLock.lock();
            final Runnable r = () -> {
                try {
                    writeTable(store3, table3);
                    writeLock.unlock();
                } catch (Exception e) {
                }
            };
            final Thread t = new Thread(r);
            t.start();
            assertEquals(2 * SIZE_OF_TABLE, cache.size());
            await().pollDelay(ONE_HUNDRED_MILLISECONDS).until(() -> t.getState() == Thread.State.WAITING);
            assertEquals(2 * SIZE_OF_TABLE, cache.size());

            // wait for flush
            flushLock.unlock();
            writeLock.lock();
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();

            assertEquals(2, checkRefs(table1)); // held once in the delegate and once in the cache
            assertEquals(1, checkRefs(table2)); // held in the delegate
            assertEquals(2, checkRefs(table3)); // held once in the delegate and once in the cache
            assertEquals(2 * SIZE_OF_TABLE, cache.size());
        }
    }

    @Test(timeout = 60_000)
    public void testCacheEmptyAfterClear() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        final CachedColumnReadStoreCache cache = generateCache(1);

        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, cache)) {

            writeTable(store, table);

            // wait for flush of table
            AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
            }).get();
            assertEquals(SIZE_OF_TABLE, cache.size());
            assertEquals(2, checkRefs(table)); // held once in the delegate and once in the cache
        }

        assertEquals(0, cache.size());
        assertEquals(0, checkRefs(table));
    }

    @Test(timeout = 60_000)
    public void testCacheNotEmptyAfterPartialClear() throws Exception {

        final List<TestColumnData[]> table1 = generateTable();
        final List<TestColumnData[]> table2 = generateTable();
        final ColumnStoreSchema schema = generateSchema();
        final CachedColumnReadStoreCache cache = generateCache(2);

        try (final ColumnStore delegate1 = new InMemoryColumnStore(schema);
                final ColumnStore store1 = new AsyncFlushCachedColumnStore(delegate1, cache)) {

            writeTable(store1, table1);

            try (final ColumnStore delegate2 = new InMemoryColumnStore(schema);
                    final ColumnStore store2 = new AsyncFlushCachedColumnStore(delegate2, cache)) {

                writeTable(store2, table2);

                // wait for flush of tables
                AsyncFlushCachedColumnStore.EXECUTOR.submit(() -> {
                }).get();
                assertEquals(2 * SIZE_OF_TABLE, cache.size());
                assertEquals(2, checkRefs(table1)); // held once in the delegate and once in the cache
                assertEquals(2, checkRefs(table2)); // held once in the delegate and once in the cache
            }

            assertEquals(SIZE_OF_TABLE, cache.size());
            assertEquals(2, checkRefs(table1));
            assertEquals(0, checkRefs(table2));
        }

        assertEquals(0, cache.size());
        assertEquals(0, checkRefs(table1));
        assertEquals(0, checkRefs(table2));
    }

    @Test
    public void testFactorySingleton() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1))) {
            assertEquals(store.getFactory(), store.getFactory());
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testWriterSingleton() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1))) {
            assertEquals(store.getWriter(), store.getWriter());
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
            store.close();
            store.getFactory().create();
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.close();
                writer.write(createBatch(1, 1));
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
            store.close();
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
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
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
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
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
            store.close();
            store.getFactory();
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
            store.close();
            store.getWriter();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1))) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.saveToFile(null);
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            store.close();
            store.saveToFile(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema());
                final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1))) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                try (final ColumnDataReader reader = store.createReader()) {
                }
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = new InMemoryColumnStore(generateSchema())) {
            final ColumnStore store = new AsyncFlushCachedColumnStore(delegate, generateCache(1));
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            store.close();
            try (final ColumnDataReader reader = store.createReader()) {
            }
        }
    }

}
