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

import static org.knime.core.columnar.TestColumnStoreUtils.createDefaultTestColumnStore;
import static org.knime.core.columnar.TestColumnStoreUtils.createDefaultTestTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readSelectionAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.readTwiceAndCompareTable;
import static org.knime.core.columnar.TestColumnStoreUtils.writeTable;
import static org.knime.core.columnar.cache.AsyncFlushCachedColumnStoreTest.checkCached;
import static org.knime.core.columnar.cache.AsyncFlushCachedColumnStoreTest.checkUncached;
import static org.knime.core.columnar.cache.AsyncFlushCachedColumnStoreTest.generateCache;

import java.io.Closeable;
import java.io.IOException;

import org.junit.Test;
import org.knime.core.columnar.TestColumnStoreUtils.TestDataTable;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestColumnStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class CachedColumnReadStoreTest extends ColumnarTest {

    private static final class StoreAndTable implements Closeable {
        private final CachedColumnReadStore m_store;

        private final TestDataTable m_table;

        StoreAndTable(final CachedColumnReadStore store, final TestDataTable table) {
            m_store = store;
            m_table = table;
        }

        @Override
        public void close() throws IOException {
            m_table.close();
            m_store.close();
        }
    }

    @SuppressWarnings("resource")
    private static StoreAndTable generateDefaultCachedColumnReadStore() throws IOException {
        final TestColumnStore delegate = createDefaultTestColumnStore();
        final TestDataTable table = createDefaultTestTable(delegate);
        writeTable(delegate, table);
        return new StoreAndTable(new CachedColumnReadStore(delegate, generateCache(1)), table);
    }

    @Test
    public void testRead() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultCachedColumnReadStore()) {
            checkUncached(storeAndTable.m_table);
            try (final TestDataTable reassembledTable =
                readAndCompareTable(storeAndTable.m_store, storeAndTable.m_table)) {
                checkCached(reassembledTable);
            }
        }
    }

    @Test
    public void testMultiRead() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultCachedColumnReadStore()) {
            checkUncached(storeAndTable.m_table);
            readTwiceAndCompareTable(storeAndTable.m_store);
        }
    }

    @Test
    public void testReadSelection() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultCachedColumnReadStore()) {
            checkUncached(storeAndTable.m_table);
            readTwiceAndCompareTable(storeAndTable.m_store);
            for (int i = 0; i < storeAndTable.m_store.getSchema().getNumColumns(); i++) {
                try (final TestDataTable reassembledTable =
                    readSelectionAndCompareTable(storeAndTable.m_store, storeAndTable.m_table, i)) { // NOSONAR
                }
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultCachedColumnReadStore()) {
            storeAndTable.close(); // NOSONAR
            try (final ColumnDataReader reader = storeAndTable.m_store.createReader()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultCachedColumnReadStore()) {
            try (final ColumnDataReader reader = storeAndTable.m_store.createReader()) {
                reader.close(); // NOSONAR
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws IOException {
        try (final StoreAndTable storeAndTable = generateDefaultCachedColumnReadStore()) {
            try (final ColumnDataReader reader = storeAndTable.m_store.createReader()) {
                storeAndTable.close(); // NOSONAR
                reader.readRetained(0);
            }
        }
    }

}
