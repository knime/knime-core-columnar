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
import static org.knime.core.columnar.cache.CacheTestUtils.checkRefs;
import static org.knime.core.columnar.cache.CacheTestUtils.createBatch;
import static org.knime.core.columnar.cache.CacheTestUtils.createSchema;
import static org.knime.core.columnar.cache.CacheTestUtils.createTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.cache.CacheTestUtils.writeTable;

import java.util.List;

import org.junit.Test;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.cache.CacheTestUtils.TestColumnData;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class InMemoryColumnStoreTest {

    private static final int TABLE_HEIGHT = 2;

    private static final int TABLE_WIDTH = 2;

    private static final int SIZE_OF_COLUMN_DATA = 1;

    private static List<TestColumnData[]> generateTable() {
        return createTable(TABLE_HEIGHT, TABLE_WIDTH, SIZE_OF_COLUMN_DATA);
    }

    private static ColumnStoreSchema generateSchema() {
        return createSchema(TABLE_WIDTH);
    }

    @Test
    public void testSizeOf() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        int sizeOf = 0;
        for (TestColumnData[] batch : table) {
            for (ColumnData data : batch) {
                sizeOf += data.sizeOf();
            }
        }

        try (final InMemoryColumnStore store = new InMemoryColumnStore(generateSchema())) {
            writeTable(store, table);
            assertEquals(sizeOf, store.sizeOf());
        }
    }

    @Test
    public void testRetain() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        assertEquals(0, checkRefs(table));

        try (final InMemoryColumnStore store = new InMemoryColumnStore(generateSchema())) {
            writeTable(store, table);
            assertEquals(1, checkRefs(table));

            store.retain();
            assertEquals(2, checkRefs(table));
        }
    }

    @Test
    public void testRelease() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        assertEquals(0, checkRefs(table));

        try (final InMemoryColumnStore store = new InMemoryColumnStore(generateSchema())) {
            writeTable(store, table);
            assertEquals(1, checkRefs(table));

            store.release();
            assertEquals(0, checkRefs(table));
        }
    }

    @Test
    public void testWriteRead() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        assertEquals(0, checkRefs(table));

        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            writeTable(store, table);
            assertEquals(1, checkRefs(table));

            readAndCompareTable(store, table);
            assertEquals(1, checkRefs(table));
        }
        assertEquals(0, checkRefs(table));
    }

    @Test
    public void testWriteMultiRead() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        assertEquals(0, checkRefs(table));

        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            writeTable(store, table);
            assertEquals(1, checkRefs(table));

            readTwiceAndCompareTable(store);
            assertEquals(1, checkRefs(table));
        }
        assertEquals(0, checkRefs(table));
    }

    @Test
    public void testWriteReadSelection() throws Exception {

        final List<TestColumnData[]> table = generateTable();
        assertEquals(0, checkRefs(table));

        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            writeTable(store, table);
            assertEquals(1, checkRefs(table));

            readSelectionAndCompareTable(store, table, 0);
            assertEquals(1, checkRefs(table));
        }
        assertEquals(0, checkRefs(table));
    }

    @Test
    public void testFactorySingleton() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            assertEquals(store.getFactory(), store.getFactory());
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testWriterSingleton() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            assertEquals(store.getWriter(), store.getWriter());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            store.close();
            store.getFactory().create();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.close();
                writer.write(createBatch(1, 1));
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            store.close();
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            try (ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            try (final ColumnDataReader reader = store.createReader()) {
                reader.close();
                reader.read(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            try (ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            try (final ColumnDataReader reader = store.createReader()) {
                store.close();
                reader.read(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            store.close();
            store.getFactory();
        }
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            store.close();
            store.getWriter();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                store.saveToFile(null);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            store.close();
            store.saveToFile(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                try (final ColumnDataReader reader = store.createReader()) {
                }
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
        try (final ColumnStore store = new InMemoryColumnStore(generateSchema())) {
            try (final ColumnDataWriter writer = store.getWriter()) {
                writer.write(createBatch(1, 1));
            }
            store.close();
            try (final ColumnDataReader reader = store.createReader()) {
            }
        }
    }

}
