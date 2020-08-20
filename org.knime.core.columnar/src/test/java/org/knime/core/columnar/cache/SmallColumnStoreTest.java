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
import static org.knime.core.columnar.TestColumnStoreUtils.createBatch;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDefaultTable;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDefaultTestColumnStore;
import static org.knime.core.columnar.TestColumnStoreUtils.generateDoubleSizedDefaultTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.tableInStore;
import static org.knime.core.columnar.TestColumnStoreUtils.writeTable;

import java.util.List;

import org.junit.Test;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.TestDoubleColumnData;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
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

    @Test
    public void testSmallWriteRead() throws Exception {

        final List<TestDoubleColumnData[]> smallTable = generateDefaultTable();
        assertEquals(1, checkRefs(smallTable));

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {

            writeTable(store, smallTable);
            assertFalse(tableInStore(delegate, smallTable));
            assertEquals(2, checkRefs(smallTable));

            readAndCompareTable(store, smallTable);
            assertEquals(2, checkRefs(smallTable));
        }
        assertEquals(1, checkRefs(smallTable));
    }

    @Test
    public void testSmallWriteMultiRead() throws Exception {

        final List<TestDoubleColumnData[]> smallTable = generateDefaultTable();
        assertEquals(1, checkRefs(smallTable));

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {

            writeTable(store, smallTable);
            assertFalse(tableInStore(delegate, smallTable));
            assertEquals(2, checkRefs(smallTable));

            readTwiceAndCompareTable(store);
            assertEquals(2, checkRefs(smallTable));
        }
        assertEquals(1, checkRefs(smallTable));
    }

    @Test
    public void testSmallWriteReadSelection() throws Exception {

        final List<TestDoubleColumnData[]> smallTable = generateDefaultTable();
        assertEquals(1, checkRefs(smallTable));

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {

            writeTable(store, smallTable);
            assertFalse(tableInStore(delegate, smallTable));
            assertEquals(2, checkRefs(smallTable));

            readSelectionAndCompareTable(store, smallTable, 0);
            assertEquals(2, checkRefs(smallTable));
        }
        assertEquals(1, checkRefs(smallTable));
    }

    @Test
    public void testLargeWriteRead() throws Exception {

        final List<TestDoubleColumnData[]> largeTable = generateDoubleSizedDefaultTable();
        assertEquals(1, checkRefs(largeTable));

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {

            writeTable(store, largeTable);
            assertTrue(tableInStore(delegate, largeTable));
            assertEquals(1, checkRefs(largeTable));

            readAndCompareTable(store, largeTable);
            assertEquals(1, checkRefs(largeTable));
        }
        assertEquals(1, checkRefs(largeTable));
    }

    @Test
    public void testSmallWriteEvictRead() throws Exception {

        final List<TestDoubleColumnData[]> smallTable1 = generateDefaultTable();
        final List<TestDoubleColumnData[]> smallTable2 = generateDefaultTable();
        assertEquals(1, checkRefs(smallTable1));
        assertEquals(1, checkRefs(smallTable2));

        final SmallColumnStoreCache cache = generateCache();

        try (final ColumnStore delegate1 = generateDefaultTestColumnStore();
                final ColumnStore store1 = new SmallColumnStore(delegate1, cache);
                final ColumnStore delegate2 = generateDefaultTestColumnStore();
                final ColumnStore store2 = new SmallColumnStore(delegate2, cache)) {

            writeTable(store1, smallTable1);
            assertFalse(tableInStore(delegate1, smallTable1));
            assertEquals(2, checkRefs(smallTable1));
            writeTable(store2, smallTable2);
            assertTrue(tableInStore(delegate1, smallTable1));
            assertEquals(1, checkRefs(smallTable1));
            assertFalse(tableInStore(delegate2, smallTable2));
            assertEquals(2, checkRefs(smallTable2));

            readAndCompareTable(store1, smallTable1);
            assertEquals(1, checkRefs(smallTable1));
            readAndCompareTable(store2, smallTable2);
            assertEquals(2, checkRefs(smallTable2));
        }
        assertEquals(1, checkRefs(smallTable1));
        assertEquals(1, checkRefs(smallTable2));
    }

    @Test
    public void testSmallWriteEvictSaveRead() throws Exception {

        final List<TestDoubleColumnData[]> smallTable1 = generateDefaultTable();
        final List<TestDoubleColumnData[]> smallTable2 = generateDefaultTable();
        assertEquals(1, checkRefs(smallTable1));
        assertEquals(1, checkRefs(smallTable2));

        final SmallColumnStoreCache cache = generateCache();

        try (final ColumnStore delegate1 = generateDefaultTestColumnStore();
                final ColumnStore store1 = new SmallColumnStore(delegate1, cache);
                final ColumnStore delegate2 = generateDefaultTestColumnStore();
                final ColumnStore store2 = new SmallColumnStore(delegate2, cache)) {

            writeTable(store1, smallTable1);
            assertFalse(tableInStore(delegate1, smallTable1));
            assertEquals(2, checkRefs(smallTable1));
            writeTable(store2, smallTable2);
            assertTrue(tableInStore(delegate1, smallTable1));
            assertEquals(1, checkRefs(smallTable1));
            assertFalse(tableInStore(delegate2, smallTable2));
            assertEquals(2, checkRefs(smallTable2));

            try {
                store1.saveToFile(null);
            } catch (UnsupportedOperationException e) {
            }
            try {
                store2.saveToFile(null);
            } catch (UnsupportedOperationException e) {
            }

            readAndCompareTable(store1, smallTable1);
            assertEquals(1, checkRefs(smallTable1));
            readAndCompareTable(store2, smallTable2);
            assertEquals(2, checkRefs(smallTable2));
        }
        assertEquals(1, checkRefs(smallTable1));
        assertEquals(1, checkRefs(smallTable2));
    }

    @Test
    public void testSmallWriteSaveEvictRead() throws Exception {

        final List<TestDoubleColumnData[]> smallTable1 = generateDefaultTable();
        final List<TestDoubleColumnData[]> smallTable2 = generateDefaultTable();
        assertEquals(1, checkRefs(smallTable1));
        assertEquals(1, checkRefs(smallTable2));

        final SmallColumnStoreCache cache = generateCache();

        try (final ColumnStore delegate1 = generateDefaultTestColumnStore();
                final ColumnStore store1 = new SmallColumnStore(delegate1, cache);
                final ColumnStore delegate2 = generateDefaultTestColumnStore();
                final ColumnStore store2 = new SmallColumnStore(delegate2, cache)) {

            writeTable(store1, smallTable1);
            assertFalse(tableInStore(delegate1, smallTable1));
            assertEquals(2, checkRefs(smallTable1));

            try {
                store1.saveToFile(null);
            } catch (UnsupportedOperationException e) {
            }

            writeTable(store2, smallTable2);
            assertTrue(tableInStore(delegate1, smallTable1));
            assertEquals(1, checkRefs(smallTable1));
            assertFalse(tableInStore(delegate2, smallTable2));
            assertEquals(2, checkRefs(smallTable2));

            try {
                store2.saveToFile(null);
            } catch (UnsupportedOperationException e) {
            }

            readAndCompareTable(store1, smallTable1);
            assertEquals(1, checkRefs(smallTable1));
            readAndCompareTable(store2, smallTable2);
            assertEquals(2, checkRefs(smallTable2));
        }
        assertEquals(1, checkRefs(smallTable1));
        assertEquals(1, checkRefs(smallTable2));
    }

    @Test
    public void testCacheEmptyAfterClear() throws Exception {

        final List<TestDoubleColumnData[]> smallTable = generateDefaultTable();
        final SmallColumnStoreCache cache = generateCache();

        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = new SmallColumnStore(delegate, cache)) {

            writeTable(store, smallTable);
            assertEquals(1, cache.size());
        }
        assertEquals(0, cache.size());
    }

    @Test
    public void testFactorySingleton() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {
            assertEquals(store.getFactory(), store.getFactory());
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testWriterSingleton() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {
            assertEquals(store.getWriter(), store.getWriter());
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
            store.close();
            store.getFactory().create();
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
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
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
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
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
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
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
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
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
            store.close();
            store.getFactory();
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
            store.close();
            store.getWriter();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore();
                final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.saveToFile(null);
            }
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws Exception {
        try (final ColumnStore delegate = generateDefaultTestColumnStore()) {
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
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
                final ColumnStore store = new SmallColumnStore(delegate, generateCache())) {
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
            final ColumnStore store = new SmallColumnStore(delegate, generateCache());
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            store.close();
            try (final ColumnDataReader reader = store.createReader()) {
            }
        }
    }

}
