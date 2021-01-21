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

import static org.knime.core.columnar.TestColumnStoreUtils.createDefaultTestColumnStore;
import static org.knime.core.columnar.TestColumnStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.releaseTable;
import static org.knime.core.columnar.TestColumnStoreUtils.writeDefaultTable;
import static org.knime.core.columnar.cache.heap.HeapCachedColumnStoreTest.generateCache;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.ColumnStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class HeapCachedColumnReadStoreTest {

    private static final class StoreAndTable implements Closeable {
        private final HeapCachedColumnReadStore m_store;

        private final List<NullableReadData[]> m_table;

        StoreAndTable(final HeapCachedColumnReadStore store, final List<NullableReadData[]> table) {
            m_store = store;
            m_table = table;
        }

        @Override
        public void close() throws IOException {
            m_store.close();
        }
    }

    @SuppressWarnings("resource")
    private static StoreAndTable generateDefaultHeapCachedReadStoreAndTable() throws IOException {
        final ColumnStore delegate = createDefaultTestColumnStore();
        final List<NullableReadData[]> table = writeDefaultTable(delegate);
        releaseTable(table);
        return new StoreAndTable(new HeapCachedColumnReadStore(delegate, generateCache()), table);
    }

    @Test
    public void testRead() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultHeapCachedReadStoreAndTable()) {
            readAndCompareTable(storeAndTable.m_store, storeAndTable.m_table);
        }
    }

    @Test
    public void testMultiRead() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultHeapCachedReadStoreAndTable()) {
            readTwiceAndCompareTable(storeAndTable.m_store);
        }
    }

    @Test
    public void testReadSelection() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultHeapCachedReadStoreAndTable()) {
            for (int i = 0; i < storeAndTable.m_store.getSchema().numColumns(); i++) {
                readSelectionAndCompareTable(storeAndTable.m_store, storeAndTable.m_table, i);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultHeapCachedReadStoreAndTable()) {
            storeAndTable.m_store.close();
            try (final BatchReader reader = storeAndTable.m_store.createReader()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultHeapCachedReadStoreAndTable()) {
            try (final BatchReader reader = storeAndTable.m_store.createReader()) {
                reader.close(); // NOSONAR
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultHeapCachedReadStoreAndTable()) {
            try (final BatchReader reader = storeAndTable.m_store.createReader()) {
                storeAndTable.m_store.close();
                reader.readRetained(0);
            }
        }
    }

}
