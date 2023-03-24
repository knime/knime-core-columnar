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
package org.knime.core.columnar.cache.writable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.TestBatchStoreUtils.DEF_SIZE_OF_TABLE;
import static org.knime.core.columnar.TestBatchStoreUtils.checkRefs;
import static org.knime.core.columnar.TestBatchStoreUtils.createDefaultTestColumnStore;
import static org.knime.core.columnar.TestBatchStoreUtils.createDefaultTestTable;
import static org.knime.core.columnar.TestBatchStoreUtils.createDoubleSizedDefaultTestTable;
import static org.knime.core.columnar.TestBatchStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestBatchStoreUtils.tableInStore;
import static org.knime.core.columnar.TestBatchStoreUtils.writeTable;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;
import org.knime.core.columnar.TestBatchStoreUtils.TestDataTable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestBatchStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class BatchWritableCacheTest extends ColumnarTest {

    private static SharedBatchWritableCache generateCache() {
        final SharedBatchWritableCache cache = new SharedBatchWritableCache(DEF_SIZE_OF_TABLE, DEF_SIZE_OF_TABLE, 1);
        assertEquals(DEF_SIZE_OF_TABLE, cache.getCacheSize());
        return cache;
    }

    private static BatchWritableCache generateDefaultSmallColumnStore(final BatchStore delegate) {
        return generateDefaultSmallColumnStore(delegate, generateCache());
    }

    private static BatchWritableCache generateDefaultSmallColumnStore(final BatchStore delegate,
        final SharedBatchWritableCache cache) {
        return new BatchWritableCache(delegate, delegate, cache);
    }

    private static void checkUncached(final TestDataTable table) {
        assertEquals(1, checkRefs(table));
    }

    private static void checkCached(final TestDataTable table) {
        // references are the TestDataTable and the cached batch
        assertEquals(2, checkRefs(table));
    }

    private static void checkUnflushed(final TestDataTable table, final TestBatchStore delegate) throws IOException {
        assertFalse(tableInStore(delegate, table));
    }

    private static void checkFlushed(final TestDataTable table, final TestBatchStore delegate) throws IOException {
        assertTrue(tableInStore(delegate, table));
    }

    @Test
    public void testSmallWriteRead() throws IOException {

        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final BatchWritableCache store = generateDefaultSmallColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            writeTable(store, table);
            checkCached(table);
            checkUnflushed(table, delegate);

            try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) { // NOSONAR
            }
            checkCached(table);
            checkUnflushed(table, delegate);
        }
    }

    @Test
    public void testSmallWriteMultiRead() throws IOException {

        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final BatchWritableCache store = generateDefaultSmallColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            writeTable(store, table);
            checkCached(table);
            checkUnflushed(table, delegate);

            readTwiceAndCompareTable(store, table.size());
            checkCached(table);
            checkUnflushed(table, delegate);
        }
    }

    @Test
    public void testSmallWriteReadSelection() throws IOException {

        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final BatchWritableCache store = generateDefaultSmallColumnStore(delegate);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            writeTable(store, table);
            checkCached(table);
            checkUnflushed(table, delegate);

            try (final TestDataTable reassembledTable = readSelectionAndCompareTable(store, table, 0)) { // NOSONAR
            }
            checkCached(table);
            checkUnflushed(table, delegate);
        }
    }

    @Test
    public void testLargeWriteRead() throws IOException {

        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final BatchWritableCache store = generateDefaultSmallColumnStore(delegate);
                final TestDataTable table = createDoubleSizedDefaultTestTable(delegate)) {

            writeTable(store, table);
            checkUncached(table);
            checkFlushed(table, delegate);

            try (final TestDataTable reassembledTable = readAndCompareTable(store, table)) { // NOSONAR
            }
            checkUncached(table);
            checkFlushed(table, delegate);
        }
    }

    @Test
    public void testSmallWriteEvictRead() throws IOException {

        final SharedBatchWritableCache cache = generateCache();
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final BatchWritableCache store1 = generateDefaultSmallColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestBatchStore delegate2 = createDefaultTestColumnStore();
                final BatchWritableCache store2 = generateDefaultSmallColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate2)) {

            writeTable(store1, table1);
            checkCached(table1);
            checkUnflushed(table1, delegate1);

            writeTable(store2, table2);
            checkCached(table2);
            checkUnflushed(table2, delegate2);
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try (final TestDataTable reassembledTable1 = readAndCompareTable(store1, table1)) { // NOSONAR
            }
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try (final TestDataTable reassembledTable2 = readAndCompareTable(store2, table2)) { // NOSONAR
            }
            checkCached(table2);
            checkUnflushed(table2, delegate2);
        }
    }

    // test for AP-15620
    @Test
    public void testGetReaderWhileEvict() throws IOException, ExecutionException, InterruptedException {

        final SharedBatchWritableCache cache = generateCache();
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final BatchWritableCache store1 = generateDefaultSmallColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestBatchStore delegate2 = createDefaultTestColumnStore();
                final BatchWritableCache store2 = generateDefaultSmallColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate2)) {

            writeTable(store1, table1);
            final CountDownLatch blockOnWrite1 = new CountDownLatch(1);
            delegate1.blockOnCreateWriteRead(blockOnWrite1);
            final ExecutorService executor = Executors.newFixedThreadPool(2);
            final Future<?> f1 = executor.submit(() -> {
                // writing table2 leads to eviction of table1, which leads to writing of table1 to delegate1
                // writing of table1 to delegate1 blocks until blockOnWrite1 is counted down
                writeTable(store2, table2);
                return null;
            });
            final Future<?> f2 = executor.submit(() -> {
                blockOnWrite1.countDown();
                try (final TestDataTable reassembledTable = readAndCompareTable(store1, table1)) { // NOSONAR
                }
                return null;
            });
            // This test is not 100% solid. if the second thread runs fully before the first thread even
            // commences, the reader will have been created before the table has been evicted from the small cache
            // this can lead to a false positive test result.
            f1.get();
            f2.get();
            executor.shutdown();
        }
    }

    @Test
    public void testSmallWriteEvictFlushRead() throws IOException {

        final SharedBatchWritableCache cache = generateCache();
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final BatchWritableCache store1 = generateDefaultSmallColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestBatchStore delegate2 = createDefaultTestColumnStore();
                final BatchWritableCache store2 = generateDefaultSmallColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate2)) {

            writeTable(store1, table1);
            writeTable(store2, table2);

            try {
                store1.flush();
            } catch (UnsupportedOperationException e) { // NOSONAR
            }
            flushRead(delegate1, store1, table1, delegate2, store2, table2);
        }
    }

    @Test
    public void testSmallWriteFlushEvictRead() throws IOException {

        final SharedBatchWritableCache cache = generateCache();
        try (final TestBatchStore delegate1 = createDefaultTestColumnStore();
                final BatchWritableCache store1 = generateDefaultSmallColumnStore(delegate1, cache);
                final TestDataTable table1 = createDefaultTestTable(delegate1);
                final TestBatchStore delegate2 = createDefaultTestColumnStore();
                final BatchWritableCache store2 = generateDefaultSmallColumnStore(delegate2, cache);
                final TestDataTable table2 = createDefaultTestTable(delegate2)) {

            writeTable(store1, table1);
            checkCached(table1);
            checkUnflushed(table1, delegate1);

            try {
                store1.flush();
            } catch (UnsupportedOperationException e) { // NOSONAR
            }
            checkCached(table1);
            checkFlushed(table1, delegate1);

            writeTable(store2, table2);
            checkCached(table2);
            checkUnflushed(table2, delegate2);
            flushRead(delegate1, store1, table1, delegate2, store2, table2);
        }
    }

    private static void flushRead(final TestBatchStore delegate1, final BatchWritableCache store1,
        final TestDataTable table1, final TestBatchStore delegate2, final BatchWritableCache store2,
        final TestDataTable table2) throws IOException {
        checkUncached(table1);
        checkFlushed(table1, delegate1);

        try {
            store2.flush();
        } catch (UnsupportedOperationException e) { // NOSONAR
        }
        checkCached(table2);
        checkFlushed(table2, delegate2);

        try (final TestDataTable reassembledTable1 = readAndCompareTable(store1, table1)) { // NOSONAR
        }
        checkUncached(table1);
        checkFlushed(table1, delegate1);

        try (final TestDataTable reassembledTable2 = readAndCompareTable(store2, table2)) { // NOSONAR
        }
        checkCached(table2);
        checkFlushed(table2, delegate2);
    }

    @Test
    public void testCacheEmptyAfterClear() throws IOException {

        final SharedBatchWritableCache cache = generateCache();
        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final BatchWritableCache store = generateDefaultSmallColumnStore(delegate, cache);
                final TestDataTable table = createDefaultTestTable(delegate)) {

            writeTable(store, table);
            assertEquals(1, cache.size());
        }
        assertEquals(0, cache.size());
    }

    @Test
    public void testWriterSingleton() throws IOException {
        try (final TestBatchStore delegate = createDefaultTestColumnStore();
                final BatchWritableCache store = generateDefaultSmallColumnStore(delegate);
                final BatchWriter writer1 = store.getWriter();
                final BatchWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }
    }

}
