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
 *   15 Dec 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.cache.heap;

import static org.junit.Assert.assertEquals;
import static org.knime.core.columnar.TestColumnStoreUtils.DEF_SIZE_OF_DATA;
import static org.knime.core.columnar.TestColumnStoreUtils.createDefaultTestColumnStore;
import static org.knime.core.columnar.TestColumnStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.releaseTable;
import static org.knime.core.columnar.TestColumnStoreUtils.writeDefaultTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.Test;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class HeapCachedColumnStoreTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    @AfterClass
    public static void tearDownTests() {
        EXECUTOR.shutdown();
    }

    static ObjectDataCache generateCache() {

        return new ObjectDataCache() {
            private final Map<ColumnDataUniqueId, Object[]> m_cache = new ConcurrentHashMap<>();

            @Override
            public Map<ColumnDataUniqueId, Object[]> getCache() {
                return m_cache;
            }
        };
    }

    private static HeapCachedColumnStore generateDefaultHeapCachedStore() {
        return new HeapCachedColumnStore(createDefaultTestColumnStore(), generateCache(), EXECUTOR);
    }

    @Test
    public void testWriteRead() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            final List<ColumnReadData[]> table = writeDefaultTable(store);
            readAndCompareTable(store, table);
            releaseTable(table);
        }
    }

    @Test
    public void testWriteMultiRead() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            final List<ColumnReadData[]> table = writeDefaultTable(store);
            readTwiceAndCompareTable(store);
            releaseTable(table);
        }
    }

    @Test
    public void testWriteReadSelection() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            final List<ColumnReadData[]> table = writeDefaultTable(store);
            for (int i = 0; i < store.getSchema().getNumColumns(); i++) {
                readSelectionAndCompareTable(store, table, i);
            }
            releaseTable(table);
        }
    }

    @Test
    public void testFactorySingleton() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            assertEquals(store.getFactory(), store.getFactory());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterWriterClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            store.getFactory();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterStoreClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            store.close();
            store.getFactory();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterWriterClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            final ColumnDataFactory factory = store.getFactory();
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            factory.create(DEF_SIZE_OF_DATA);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            final ColumnDataFactory factory = store.getFactory();
            store.close();
            factory.create(DEF_SIZE_OF_DATA);
        }
    }

    @Test
    public void testWriterSingleton() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore();
                final ColumnDataWriter writer1 = store.getWriter();
                final ColumnDataWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterWriterClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            store.close();
            try (final ColumnDataWriter writer = store.getWriter()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            store.getWriter().close();
            releaseTable(writeDefaultTable(store));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            store.close();
            releaseTable(writeDefaultTable(store));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            store.save(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            releaseTable(writeDefaultTable(store));
            store.close();
            store.save(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            try (final ColumnDataReader reader = store.createReader()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            releaseTable(writeDefaultTable(store));
            store.close();
            try (final ColumnDataReader reader = store.createReader()) {
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            releaseTable(writeDefaultTable(store));
            try (final ColumnDataReader reader = store.createReader()) {
                reader.close();
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws Exception {
        try (final ColumnStore store = generateDefaultHeapCachedStore()) {
            releaseTable(writeDefaultTable(store));
            try (final ColumnDataReader reader = store.createReader()) {
                store.close();
                reader.readRetained(0);
            }
        }
    }

}
