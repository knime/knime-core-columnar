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
 *   23 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.batch;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.Test;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.TestBatchStoreUtils;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.columnar.testing.data.TestData;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public abstract class AbstractBatchTest<B extends AbstractBatch<? extends ReferencedData>> {

    @SuppressWarnings("resource")
    static TestData[] createData() {
        final ColumnarSchema schema = TestBatchStoreUtils.createSchema(DEF_NUM_COLUMNS);
        final TestBatchStore store = TestBatchStore.create(schema);
        return TestBatchStoreUtils.createTestTable(store, 1).get(0);
    }

    static final int DEF_NUM_COLUMNS = 2;

    abstract B createBatch(final TestData[] data);

    @Test(expected = NullPointerException.class)
    public void testArrayNullCheckOnCreate() {
        createBatch(null);
    }

    @Test(expected = NullPointerException.class)
    public void testElementNullCheckOnCreate() {
        final TestData[] data = createData();
        data[0] = null;
        createBatch(data);
    }

    @Test
    public void testGet() {
        final TestData[] data = createData();
        final B batch = createBatch(data);
        for (int i = 0; i < DEF_NUM_COLUMNS; i++) {
            assertEquals(data[i], batch.get(i));
            assertArrayEquals(data, batch.getUnsafe());
        }
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
        final B batch = createBatch(data);
        for (int i = 0; i < DEF_NUM_COLUMNS; i++) {
            assertEquals(1, data[i].getRefs());
        }
        batch.retain();
        for (int i = 0; i < DEF_NUM_COLUMNS; i++) {
            // batches count their own references and only release once their reference count hits zero
            assertEquals(1, data[i].getRefs());
        }
        batch.release();
        for (int i = 0; i < DEF_NUM_COLUMNS; i++) {
            assertEquals(1, data[i].getRefs());
        }
        batch.release();
        Stream.of(data).forEach(d -> assertEquals(0, d.getRefs()));
    }

    @Test
    public void testSizeOf() {
        final TestData[] data = createData();
        assertEquals(Arrays.stream(data).mapToLong(ReferencedData::sizeOf).sum(), createBatch(data).sizeOf());
    }

}
