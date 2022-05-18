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
package org.knime.core.columnar.cache.data;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.EvictingCache;
import org.knime.core.columnar.cache.EvictingCache.Evictor;
import org.knime.core.columnar.cache.writable.SharedBatchWritableCache;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link RandomAccessBatchReadable} that reads {@link ReadData} from a delegate and places it in a fixed-size
 * {@link SharedBatchWritableCache LRU cache} in memory for faster subsequent access.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ReadDataReadCache implements RandomAccessBatchReadable {

    private final class ReadDataReadCacheReader implements RandomAccessBatchReader {

        private final Evictor<ColumnDataUniqueId, NullableReadData> m_evictor = (k, c) -> m_cachedData.remove(k);

        private final ColumnSelection m_selection;

        ReadDataReadCacheReader(final ColumnSelection selection) {
            m_selection = selection;
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            return m_selection.createBatch(i -> { // NOSONAR
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(ReadDataReadCache.this, i, index);
                final Object lock = m_cachedData.computeIfAbsent(ccUID, k -> new Object());

                synchronized (lock) {
                    final NullableReadData cachedData = m_globalCache.getRetained(ccUID);
                    if (cachedData != null) {
                        return cachedData;
                    }

                    try (RandomAccessBatchReader reader = m_reabableDelegate
                        .createRandomAccessReader(new FilteredColumnSelection(m_selection.numColumns(), i))) {
                        final NullableReadData data = reader.readRetained(index).get(i);
                        m_globalCache.put(ccUID, data, m_evictor);
                        return data;
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }
            });
        }

        @Override
        public void close() throws IOException {
            // no resources held
        }

    }

    private final RandomAccessBatchReadable m_reabableDelegate;

    private final EvictingCache<ColumnDataUniqueId, NullableReadData> m_globalCache;

    private Map<ColumnDataUniqueId, Object> m_cachedData = new ConcurrentHashMap<>();

    /**
     * @param reabableDelegate the delegate from which to read in case of a cache miss
     * @param cache the cache for obtaining and storing data
     */
    public ReadDataReadCache(final RandomAccessBatchReadable reabableDelegate, final SharedReadDataCache cache) {
        m_reabableDelegate = reabableDelegate;
        m_globalCache = cache.getCache();
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new ReadDataReadCacheReader(selection);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_reabableDelegate.getSchema();
    }

    @Override
    public synchronized void close() throws IOException {
        if (m_cachedData != null) {
            for (final ColumnDataUniqueId id : m_cachedData.keySet()) {
                m_globalCache.remove(id);
            }
            m_cachedData.clear();
            m_cachedData = null;
            m_reabableDelegate.close();
        }
    }
}
