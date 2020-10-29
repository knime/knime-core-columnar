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
import static org.junit.Assert.assertNull;
import static org.knime.core.columnar.TestColumnStoreUtils.createSchema;
import static org.knime.core.columnar.TestColumnStoreUtils.createTable;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.knime.core.columnar.TestColumnStoreUtils.TestTable;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestColumnStore;
import org.knime.core.columnar.testing.TestDoubleData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class SizeBoundLruCacheTest extends ColumnarTest {

    @Test
    public void testPutGet() throws Exception {

        final LoadingEvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(1);
        try (final TestColumnStore store = TestColumnStore.create(createSchema(1), 1);
                final TestTable table = createTable(store, 1)) {
            final TestDoubleData data = table.getBatch(0)[0];

            assertEquals(1, data.getRefs());

            cache.put(0, data);
            assertEquals(1, cache.size());
            assertEquals(2, data.getRefs());

            assertEquals(data, cache.getRetained(0));
            assertEquals(3, data.getRefs());

            assertEquals(data, cache.getRetained(0, () -> null, (i, d) -> {
            }));
            assertEquals(4, data.getRefs());

            cache.removeRetained(0).release();
            data.release();
            data.release();
        }
    }

    @Test
    public void testPutEvictLoadGet() throws Exception {

        final LoadingEvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(1);
        try (final TestColumnStore store = TestColumnStore.create(createSchema(2), 1);
                final TestTable table = createTable(store, 1)) {
            final TestDoubleData[] batch = table.getBatch(0);

            assertEquals(1, batch[0].getRefs());

            final AtomicBoolean evicted = new AtomicBoolean();
            cache.put(0, batch[0], (i, d) -> {
                evicted.set(true);
            });
            assertEquals(1, cache.size());
            assertEquals(2, batch[0].getRefs());

            cache.put(1, batch[1]);
            assertEquals(true, evicted.get());
            assertEquals(1, cache.size());
            assertEquals(1, batch[0].getRefs());

            assertNull(cache.getRetained(0));
            assertEquals(batch[0], cache.getRetained(0, () -> {
                batch[0].retain();
                return batch[0];
            }, (i, d) -> {
            }));
            assertEquals(3, batch[0].getRefs());

            cache.removeRetained(0).release();
            batch[0].release();
        }
    }

    @Test
    public void testPutRemove() throws Exception {

        final LoadingEvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(1);
        try (final TestColumnStore store = TestColumnStore.create(createSchema(1), 1);
                final TestTable table = createTable(store, 1)) {
            final TestDoubleData data = table.getBatch(0)[0];

            assertEquals(1, data.getRefs());

            cache.put(0, data);
            assertEquals(1, cache.size());
            assertEquals(2, data.getRefs());

            assertEquals(data, cache.removeRetained(0));
            assertEquals(2, data.getRefs());

            assertNull(cache.removeRetained(0));

            data.release();
        }
    }

    @Test
    public void testLru() throws Exception {
        final LoadingEvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(2);
        try (final TestColumnStore store = TestColumnStore.create(createSchema(3), 1);
                final TestTable table = createTable(store, 1)) {
            final TestDoubleData[] batch = table.getBatch(0);

            cache.put(0, batch[0]); // content in cache: 0
            cache.put(1, batch[1]); // content in cache: 1->0
            cache.put(2, batch[2]); // content in cache: 2->1
            assertEquals(2, cache.size());

            assertEquals(batch[2], cache.getRetained(2)); // content in cache: 2->1
            batch[2].release();
            assertEquals(batch[1], cache.getRetained(1)); // content in cache: 1->2
            batch[1].release();
            assertNull(cache.getRetained(0));

            cache.put(0, batch[0]); // content in cache: 0->1
            assertEquals(batch[0], cache.getRetained(0));
            batch[0].release();
            assertEquals(batch[1], cache.getRetained(1));
            batch[1].release();
            assertNull(cache.getRetained(2));

            cache.removeRetained(0).release();
            cache.removeRetained(1).release();
        }
    }

}
