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
 *   Dec 17, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.object;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.DataIndex;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Contains unit tests for CacheManager.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@RunWith(MockitoJUnitRunner.class)
public class CacheManagerTest {

    private TestSharedObjectCache m_cache;

    private RandomAccessBatchReadable m_batchReadable;

    private CacheManager m_testInstance;

    @Before
    public void before() {
        m_cache = new TestSharedObjectCache();
        m_batchReadable = new DummyRandomAccessBatchReadable();
        m_testInstance = new CacheManager(m_cache);
    }

    @Test
    public void testCacheData() {
        final var values = new String[] {"foo", "bar"};
        final var id = createId(null, 0);
        m_testInstance.cacheData(values, id);
        assertEquals(1, m_cache.m_cache.size());
        assertTrue(m_cache.m_cache.containsKey(id));
    }

    @Test
    public void testGetOrCreatePresentValue() {
        var values = new String[] {"foo", "bar"};
        var index = createId(0, 0);
        m_testInstance.cacheData(values, index);
        assertTrue(m_cache.m_cache.containsKey(index));
        assertArrayEquals(values, (String[])m_testInstance.getOrCreate(index, () -> values));
        assertTrue(m_cache.m_cache.containsKey(index));
    }

    private ColumnDataUniqueId createId(final DataIndex index, final int batchIndex) {
        return new ColumnDataUniqueId(m_batchReadable, index, batchIndex);
    }

    private ColumnDataUniqueId createId(final int columnIndex, final int batchIndex) {
        return new ColumnDataUniqueId(m_batchReadable, columnIndex, batchIndex);
    }

    @Test
    public void testGetOrCreateAbsentValue() {
        var values = new String[] {"foo", "bar"};
        var index = createId(0, 0);
        assertEquals(0, m_cache.m_cache.size());
        m_testInstance.getOrCreate(index, () -> values);
        assertTrue(m_cache.m_cache.containsKey(index));

    }

    @Test
    public void testClose() {
        String[][] values = { {"foo", "bar"}, {"bla", "baz"}};
        ColumnDataUniqueId[] indices = new ColumnDataUniqueId[] {createId(0, 0), createId(1, 0)};
        m_testInstance.cacheData(values[0], indices[0]);
        m_testInstance.cacheData(values[1], indices[1]);
        assertEquals(2, m_cache.m_cache.size());
        m_testInstance.close();
        assertEquals(0, m_cache.m_cache.size());
    }

    private static final class TestSharedObjectCache implements SharedObjectCache {

        private final Map<ColumnDataUniqueId, Object> m_cache = new ConcurrentHashMap<>();

        @Override
        public Object computeIfAbsent(final ColumnDataUniqueId key,
            final Function<ColumnDataUniqueId, Object> mappingFunction) {
            return m_cache.computeIfAbsent(key, mappingFunction);
        }

        @Override
        public void put(final ColumnDataUniqueId key, final Object value) {
            m_cache.put(key, value);
        }

        @Override
        public void removeAll(final Collection<ColumnDataUniqueId> keys) {
            m_cache.keySet().removeAll(keys);
        }

    }

    private static final class DummyRandomAccessBatchReadable implements RandomAccessBatchReadable {

        @Override
        public ColumnarSchema getSchema() {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
            return null;
        }

        @Override
        public long[] getBatchBoundaries() {
            return new long[0];
        }

    }

}
