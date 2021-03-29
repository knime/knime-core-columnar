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
package org.knime.core.data.columnar.table;

import static org.knime.core.data.columnar.TestDataBatchStoreUtils.readDefaultTable;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.writeDefaultTable;
import static org.knime.core.data.columnar.table.CachedDomainDuplicateCheckBatchStoreTest.SCHEMA;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestBatchStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class CachedBatchReadStoreTest extends ColumnarTest {

    @SuppressWarnings("resource")
    private static CachedBatchReadStore generateDefaultWrappedBatchReadStore() throws IOException {
        final BatchStore store = TestBatchStore.create(SCHEMA);
        writeDefaultTable(store);
        return new CachedBatchReadStore(store);
    }

    @Test
    public void testRead() throws IOException {
        try (final CachedBatchReadStore store = generateDefaultWrappedBatchReadStore()) {
            readDefaultTable(store);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws IOException {
        try (final CachedBatchReadStore store = generateDefaultWrappedBatchReadStore()) {
            store.close();
            try (final RandomAccessBatchReader reader = store.createReader()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws IOException {
        try (final CachedBatchReadStore store = generateDefaultWrappedBatchReadStore()) {
            try (final RandomAccessBatchReader reader = store.createReader()) {
                reader.close(); // NOSONAR
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws IOException {
        try (final CachedBatchReadStore store = generateDefaultWrappedBatchReadStore()) {
            try (final RandomAccessBatchReader reader = store.createReader()) {
                store.close();
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void exceptionOnReadIndexOutOfBoundsLower() throws IOException {
        try (final CachedBatchReadStore store = generateDefaultWrappedBatchReadStore()) {
            try (final RandomAccessBatchReader reader = store.createReader()) {
                reader.readRetained(-1);
            }
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void exceptionOnReadIndexOutOfBoundsUpper() throws IOException {
        try (final CachedBatchReadStore store = generateDefaultWrappedBatchReadStore()) {
            try (final RandomAccessBatchReader reader = store.createReader()) {
                reader.readRetained(Integer.MAX_VALUE);
            }
        }
    }

}
