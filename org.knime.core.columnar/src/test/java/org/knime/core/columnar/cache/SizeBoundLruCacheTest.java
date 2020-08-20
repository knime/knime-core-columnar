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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.knime.core.columnar.cache.CacheTestUtils.TestColumnData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class SizeBoundLruCacheTest {

    @Test
    public void testPutGet() throws Exception {
        final LoadingEvictingCache<Integer, TestColumnData> cache = new SizeBoundLruCache<>(1);
        final TestColumnData data = new TestColumnData(1);
        assertEquals(0, data.getRefs());

        cache.retainAndPut(1, data);
        assertEquals(1, cache.size());
        assertEquals(1, data.getRefs());

        assertEquals(data, cache.retainAndGet(1));
        assertEquals(2, data.getRefs());

        assertEquals(data, cache.retainAndGet(1, () -> null, (i, d) -> data.release()));
        assertEquals(3, data.getRefs());
    }

    @Test
    public void testPutEvictLoadGet() throws Exception {
        final LoadingEvictingCache<Integer, TestColumnData> cache = new SizeBoundLruCache<>(1);
        final TestColumnData data1 = new TestColumnData(1);
        final TestColumnData data2 = new TestColumnData(1);
        assertEquals(0, data1.getRefs());

        final AtomicBoolean evicted = new AtomicBoolean();
        cache.retainAndPut(1, data1, (i, d) -> {
            evicted.set(true);
            d.release();
        });
        assertEquals(1, cache.size());
        assertEquals(1, data1.getRefs());

        cache.retainAndPut(2, data2);
        assertEquals(true, evicted.get());
        assertEquals(1, cache.size());
        assertEquals(0, data1.getRefs());

        assertNull(cache.retainAndGet(1));
        assertEquals(data1, cache.retainAndGet(1, () -> {
            data1.retain();
            return data1;
        }, (i, data) -> data.release()));
        assertEquals(2, data1.getRefs());
    }

    @Test
    public void testPutRemove() throws Exception {
        final LoadingEvictingCache<Integer, TestColumnData> cache = new SizeBoundLruCache<>(1);
        final TestColumnData data = new TestColumnData(1);
        assertEquals(0, data.getRefs());

        cache.retainAndPut(1, data);
        assertEquals(1, cache.size());
        assertEquals(1, data.getRefs());

        assertEquals(data, cache.remove(1));
        assertEquals(1, data.getRefs());

        assertNull(cache.remove(1));
    }

    @Test
    public void testLru() throws Exception {
        final LoadingEvictingCache<Integer, TestColumnData> cache = new SizeBoundLruCache<>(2);
        final TestColumnData data1 = new TestColumnData(1);
        final TestColumnData data2 = new TestColumnData(1);
        final TestColumnData data3 = new TestColumnData(1);

        cache.retainAndPut(1, data1); // content in cache: 1
        cache.retainAndPut(2, data2); // content in cache: 2->1
        cache.retainAndPut(3, data3); // content in cache: 3->2
        assertEquals(2, cache.size());

        assertEquals(data3, cache.retainAndGet(3)); // content in cache: 3->2
        assertEquals(data2, cache.retainAndGet(2)); // content in cache: 2->3
        assertNull(cache.retainAndGet(1));

        cache.retainAndPut(1, data1); // content in cache: 1->2
        assertEquals(data1, cache.retainAndGet(1));
        assertEquals(data2, cache.retainAndGet(2));
        assertNull(cache.retainAndGet(3));
    }

}
