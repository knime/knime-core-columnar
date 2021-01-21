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
 *   18 Jan 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.TestColumnStoreUtils.createSchema;
import static org.knime.core.columnar.TestColumnStoreUtils.createTestTable;

import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import org.junit.Test;
import org.knime.core.columnar.TestColumnStoreUtils;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.columnar.testing.TestColumnStore;
import org.knime.core.columnar.testing.data.TestData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class FilteredColumnSelectionTest {

    private static final int DEF_NUM_COLUMNS = 2;

    @SuppressWarnings("resource")
    private static TestData[] createData() {
        final ColumnStoreSchema schema = createSchema(DEF_NUM_COLUMNS);
        final TestColumnStore store = TestColumnStore.create(schema);
        return createTestTable(store, 1).get(0);
    }

    private static ReadBatch createBatch(final NullableReadData[] data, final int... indices) {
        return new FilteredColumnSelection(DEF_NUM_COLUMNS, indices).createBatch(i -> data[i]);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testNullCheckOnCreate() {
        new FilteredColumnSelection(0, null);
    }

    @Test
    public void testAllSelected() {
        final FilteredColumnSelection selection =
            new FilteredColumnSelection(DEF_NUM_COLUMNS, IntStream.range(0, DEF_NUM_COLUMNS).toArray());
        for (int i = 0; i < DEF_NUM_COLUMNS; i++) {
            assertTrue(selection.isSelected(i));
        }
    }

    @Test
    public void testNoneSelected() {
        final FilteredColumnSelection selection = new FilteredColumnSelection(DEF_NUM_COLUMNS);
        for (int i = 0; i < DEF_NUM_COLUMNS; i++) {
            assertFalse(selection.isSelected(i));
        }
    }

    @Test
    public void testGetNumColumns() {
        final int numColumns = DEF_NUM_COLUMNS;
        assertEquals(numColumns, new FilteredColumnSelection(numColumns).numColumns());
    }

    @Test(expected = NullPointerException.class)
    public void testNullCheckOnCreateBatch() {
        new FilteredColumnSelection(DEF_NUM_COLUMNS).createBatch(null);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGet() {
        final NullableReadData[] data = createData();
        final ReadBatch batch = createBatch(data, 0);
        assertEquals(data[0], batch.get(0));
        batch.get(1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexOutOfBoundsLower() {
        createBatch(createData()).get(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIndexOutOfBoundsUpper() {
        createBatch(createData()).get(DEF_NUM_COLUMNS);
    }

    @Test
    public void testRetainRelease() {
        final TestData[] data = createData();
        final ReadBatch batch = createBatch(data, 0);
        assertEquals(1, data[0].getRefs());
        batch.retain();
        assertEquals(2, data[0].getRefs());
        batch.release();
        assertEquals(1, data[0].getRefs());
    }

    @Test
    public void testSizeOf() {
        final TestData[] data = createData();
        assertEquals(data[0].sizeOf(), createBatch(data, 0).sizeOf());
    }

    @Test
    public void testGetters() {
        final int numColumns = DEF_NUM_COLUMNS;
        @SuppressWarnings("resource")
        final DefaultReadBatch batch =
            new DefaultReadBatch(createTestTable(TestColumnStore.create(createSchema(numColumns)), 1).get(0));
        assertEquals(numColumns, batch.size());
        assertEquals(TestColumnStoreUtils.DEF_SIZE_OF_DATA, batch.length());
    }

}
