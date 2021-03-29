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
import static org.knime.core.columnar.TestBatchStoreUtils.DEF_BATCH_LENGTH;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.junit.Test;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.data.TestDoubleData;
import org.knime.core.columnar.testing.data.TestDoubleData.TestDoubleDataFactory;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class SizeBoundLruCacheTest extends ColumnarTest {

    @Test
    public void testPutGet() {

        final EvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(DEF_BATCH_LENGTH, 1);
        final TestDoubleData data = TestDoubleDataFactory.INSTANCE.createWriteData(DEF_BATCH_LENGTH);

        assertEquals(1, data.getRefs());

        cache.put(0, data);
        assertEquals(1, cache.size());
        assertEquals(2, data.getRefs());

        assertEquals(data, cache.getRetained(0));
        assertEquals(3, data.getRefs());

        cache.removeRetained(0);
        assertEquals(0, cache.size());
        assertEquals(3, data.getRefs());
    }

    @Test
    public void testPutTwice() {

        final EvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(DEF_BATCH_LENGTH, 1);
        final TestDoubleData data1 = TestDoubleDataFactory.INSTANCE.createWriteData(DEF_BATCH_LENGTH);
        final TestDoubleData data2 = TestDoubleDataFactory.INSTANCE.createWriteData(DEF_BATCH_LENGTH);

        assertEquals(1, data1.getRefs());
        assertEquals(1, data2.getRefs());

        cache.put(0, data1);
        assertEquals(1, cache.size());
        assertEquals(2, data1.getRefs());

        cache.put(0, data2);
        assertEquals(1, cache.size());
        assertEquals(1, data1.getRefs());
        assertEquals(2, data2.getRefs());
    }

    @Test
    public void testPutEvictGet() {

        final EvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(DEF_BATCH_LENGTH, 1);
        final TestDoubleData[] batch =
            IntStream.range(0, 2).mapToObj(i -> TestDoubleDataFactory.INSTANCE.createWriteData(DEF_BATCH_LENGTH))
                .toArray(TestDoubleData[]::new);

        assertEquals(1, batch[0].getRefs());
        assertEquals(1, batch[1].getRefs());

        final AtomicBoolean batch0Evicted = new AtomicBoolean();
        cache.put(0, batch[0], (i, d) -> batch0Evicted.set(true));
        assertEquals(1, cache.size());
        assertEquals(2, batch[0].getRefs());

        cache.put(1, batch[1]);
        assertEquals(true, batch0Evicted.get());
        assertEquals(1, cache.size());
        assertEquals(1, batch[0].getRefs());

        assertNull(cache.getRetained(0));
        assertEquals(1, batch[0].getRefs());

        cache.removeRetained(1);
        assertEquals(0, cache.size());
        assertEquals(1, batch[0].getRefs());
        assertEquals(2, batch[1].getRefs());
    }

    @Test
    public void testPutRemove() {

        final EvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(DEF_BATCH_LENGTH, 1);
        final TestDoubleData data = TestDoubleDataFactory.INSTANCE.createWriteData(DEF_BATCH_LENGTH);

        assertEquals(1, data.getRefs());

        cache.put(0, data);
        assertEquals(1, cache.size());
        assertEquals(2, data.getRefs());

        assertEquals(data, cache.removeRetained(0));
        assertEquals(2, data.getRefs());

        assertNull(cache.removeRetained(0));
        assertEquals(0, cache.size());
        assertEquals(2, data.getRefs());
    }

    @Test
    public void testLru() {

        final EvictingCache<Integer, TestDoubleData> cache = new SizeBoundLruCache<>(DEF_BATCH_LENGTH * 2L, 1);
        final TestDoubleData[] batch =
            IntStream.range(0, 3).mapToObj(i -> TestDoubleDataFactory.INSTANCE.createWriteData(DEF_BATCH_LENGTH))
                .toArray(TestDoubleData[]::new);

        cache.put(0, batch[0]); // content in cache: 0
        cache.put(1, batch[1]); // content in cache: 1->0
        cache.put(2, batch[2]); // content in cache: 2->1
        assertEquals(2, cache.size());
        assertEquals(1, batch[0].getRefs());
        assertEquals(2, batch[1].getRefs());
        assertEquals(2, batch[2].getRefs());

        assertEquals(batch[2], cache.getRetained(2)); // content in cache: 2->1
        assertEquals(batch[1], cache.getRetained(1)); // content in cache: 1->2
        assertNull(cache.getRetained(0));
        assertEquals(1, batch[0].getRefs());
        assertEquals(3, batch[1].getRefs());
        assertEquals(3, batch[2].getRefs());

        cache.put(0, batch[0]); // content in cache: 0->1
        assertEquals(batch[0], cache.getRetained(0));
        assertEquals(batch[1], cache.getRetained(1));
        assertNull(cache.getRetained(2));
        assertEquals(3, batch[0].getRefs());
        assertEquals(4, batch[1].getRefs());
        assertEquals(2, batch[2].getRefs());
    }

}
