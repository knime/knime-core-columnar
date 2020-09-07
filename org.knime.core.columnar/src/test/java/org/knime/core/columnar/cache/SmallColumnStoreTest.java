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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.TestColumnStoreUtils.DEF_SIZE_OF_TABLE;
import static org.knime.core.columnar.TestColumnStoreUtils.checkRefs;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDefaultTable;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDefaultTestColumnStore;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDoubleSizedDefaultTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.tableInStore;
import static org.knime.core.columnar.TestColumnStoreUtils.writeTable;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.TestColumnStore;
import org.knime.core.columnar.TestColumnStoreUtils.TestTable;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class SmallColumnStoreTest {

    private static SmallColumnStoreCache generateCache() {
        return new SmallColumnStoreCache(DEF_SIZE_OF_TABLE, DEF_SIZE_OF_TABLE);
    }

    private static SmallColumnStore generateDefaultSmallColumnStore(final ColumnStore delegate) {
        return generateDefaultSmallColumnStore(delegate, generateCache());
    }

    private static SmallColumnStore generateDefaultSmallColumnStore(final ColumnStore delegate,
        final SmallColumnStoreCache cache) {
        return new SmallColumnStore(delegate, cache);
    }

    private static void checkUncached(final TestTable table) {
        assertEquals(1, checkRefs(table));
    }

    private static void checkCached(final TestTable table) {
        assertEquals(2, checkRefs(table));
    }

    private static void checkUnflushed(final TestTable table, final TestColumnStore delegate) throws IOException {
        assertFalse(tableInStore(delegate, table));
    }

    private static void checkFlushed(final TestTable table, final TestColumnStore delegate) throws IOException {
        assertTrue(tableInStore(delegate, table));
    }

    @Test
    public void testSmallWriteRead() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final SmallColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {

            writeTable(store, table);
            checkCached(table);
            checkUnflushed(table, delegate);

            try (final TestTable reassembledTable = readAndCompareTable(store, table)) {
            }
            checkCached(table);
            checkUnflushed(table, delegate);
        }
    }

    @Test
    public void testSmallWriteMultiRead() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final SmallColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {

            writeTable(store, table);
            checkCached(table);
            checkUnflushed(table, delegate);

            readTwiceAndCompareTable(store);
            checkCached(table);
            checkUnflushed(table, delegate);
        }
    }

    @Test
    public void testSmallWriteReadSelection() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final SmallColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {

            writeTable(store, table);
            checkCached(table);
            checkUnflushed(table, delegate);

            try (final TestTable reassembledTable = readSelectionAndCompareTable(store, table, 0)) {
            }
            checkCached(table);
            checkUnflushed(table, delegate);
        }
    }

    @Test
    public void testLargeWriteRead() throws Exception {

        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final SmallColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDoubleSizedDefaultTable(delegate)) {

            writeTable(store, table);
            checkUncached(table);
            checkFlushed(table, delegate);

            try (final TestTable reassembledTable = readAndCompareTable(store, table)) {
            }
            checkUncached(table);
            checkFlushed(table, delegate);
        }
    }

    @Test
    public void testSmallWriteEvictRead() throws Exception {

        final SmallColumnStoreCache cache = generateCache();
        try (final TestColumnStore delegate1 = generateDefaultTestColumnStore();
                final SmallColumnStore store1 = generateDefaultSmallColumnStore(delegate1, cache);
                final TestTable table1 = generateDefaultTable(delegate1);
                final TestColumnStore delegate2 = generateDefaultTestColumnStore();
                final SmallColumnStore store2 = generateDefaultSmallColumnStore(delegate2, cache);
                final TestTable table2 = generateDefaultTable(delegate2)) {

            writeTable(store1, table1);
            checkCached(table1);
            checkUnflushed(table1, delegate1);

            writeTable(store2, table2);
            checkCached(table2);
            checkUnflushed(table2, delegate2);
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try (final TestTable reassembledTable1 = readAndCompareTable(store1, table1)) {
            }
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try (final TestTable reassembledTable2 = readAndCompareTable(store2, table2)) {
            }
            checkCached(table2);
            checkUnflushed(table2, delegate2);
        }
    }

    @Test
    public void testSmallWriteEvictSaveRead() throws Exception {

        final SmallColumnStoreCache cache = generateCache();
        try (final TestColumnStore delegate1 = generateDefaultTestColumnStore();
                final SmallColumnStore store1 = generateDefaultSmallColumnStore(delegate1, cache);
                final TestTable table1 = generateDefaultTable(delegate1);
                final TestColumnStore delegate2 = generateDefaultTestColumnStore();
                final SmallColumnStore store2 = generateDefaultSmallColumnStore(delegate2, cache);
                final TestTable table2 = generateDefaultTable(delegate2)) {

            writeTable(store1, table1);
            checkCached(table1);
            checkUnflushed(table1, delegate1);

            writeTable(store2, table2);
            checkCached(table2);
            checkUnflushed(table2, delegate2);
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try {
                store1.saveToFile(null);
            } catch (UnsupportedOperationException e) {
            }
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try {
                store2.saveToFile(null);
            } catch (UnsupportedOperationException e) {
            }
            checkCached(table2);
            checkFlushed(table2, delegate2);

            try (final TestTable reassembledTable1 = readAndCompareTable(store1, table1)) {
            }
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try (final TestTable reassembledTable2 = readAndCompareTable(store2, table2)) {
            }
            checkCached(table2);
            checkFlushed(table2, delegate2);
        }
    }

    @Test
    public void testSmallWriteSaveEvictRead() throws Exception {

        final SmallColumnStoreCache cache = generateCache();
        try (final TestColumnStore delegate1 = generateDefaultTestColumnStore();
                final SmallColumnStore store1 = generateDefaultSmallColumnStore(delegate1, cache);
                final TestTable table1 = generateDefaultTable(delegate1);
                final TestColumnStore delegate2 = generateDefaultTestColumnStore();
                final SmallColumnStore store2 = generateDefaultSmallColumnStore(delegate2, cache);
                final TestTable table2 = generateDefaultTable(delegate2)) {

            writeTable(store1, table1);
            checkCached(table1);
            checkUnflushed(table1, delegate1);

            try {
                store1.saveToFile(null);
            } catch (UnsupportedOperationException e) {
            }
            checkCached(table1);
            checkFlushed(table1, delegate1);

            writeTable(store2, table2);
            checkCached(table2);
            checkUnflushed(table2, delegate2);
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try {
                store2.saveToFile(null);
            } catch (UnsupportedOperationException e) {
            }
            checkCached(table2);
            checkFlushed(table2, delegate2);

            try (final TestTable reassembledTable1 = readAndCompareTable(store1, table1)) {
            }
            checkUncached(table1);
            checkFlushed(table1, delegate1);

            try (final TestTable reassembledTable2 = readAndCompareTable(store2, table2)) {
            }
            checkCached(table2);
            checkFlushed(table2, delegate2);
        }
    }

    @Test
    public void testCacheEmptyAfterClear() throws Exception {

        final SmallColumnStoreCache cache = generateCache();
        try (final TestColumnStore delegate = generateDefaultTestColumnStore();
                final SmallColumnStore store = generateDefaultSmallColumnStore(delegate, cache);
                final TestTable table = generateDefaultTable(delegate)) {

            writeTable(store, table);
            assertEquals(1, cache.size());
        }
        assertEquals(0, cache.size());
    }

    @Test
    public void testFactorySingleton() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            assertEquals(store.getFactory(), store.getFactory());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            store.getFactory();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            store.close();
            store.getFactory();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            final ColumnDataFactory factory = store.getFactory();
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            factory.create();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            final ColumnDataFactory factory = store.getFactory();
            store.close();
            factory.create();
        }
    }

    @Test
    public void testWriterSingleton() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate);
                final ColumnDataWriter writer1 = store.getWriter();
                final ColumnDataWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            store.close();
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.close();
                writeTable(store, table);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.close();
                writeTable(store, table);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.saveToFile(null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            store.close();
            store.saveToFile(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate)) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                try (final ColumnDataReader reader = store.createReader()) {
                }
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
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
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            try (final ColumnDataReader reader = store.createReader()) {
                reader.close();
                reader.read(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = generateDefaultSmallColumnStore(delegate);
                final TestTable table = generateDefaultTable(delegate)) {
            try (ColumnDataWriter writer = store.getWriter()) {
                writeTable(store, table);
            }
            try (final ColumnDataReader reader = store.createReader()) {
                store.close();
                reader.read(0);
            }
        }
    }

}
