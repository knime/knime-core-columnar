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

import static org.knime.core.columnar.cache.data.ReadDataCache.getSelectedColumns;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.EvictingCache;
import org.knime.core.columnar.cache.EvictingCache.Evictor;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link RandomAccessBatchReadable} that reads {@link ReadData} from a delegate and places it in a fixed-size
 * {@link SharedReadDataCache LRU cache} in memory for faster subsequent access.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ReadDataReadCache implements RandomAccessBatchReadable {

    private final class ReadDataReadCacheReader implements RandomAccessBatchReader {

        private final Evictor<ColumnDataUniqueId, NullableReadData> m_evictor = (k, c) -> m_cachedData.remove(k);

        private final ColumnSelection m_selection;

        private final int[] m_selectedColumns;

        ReadDataReadCacheReader(final ColumnSelection selection) {
            m_selection = selection;
            m_selectedColumns = getSelectedColumns(selection);
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            final int numColumns = m_selection.numColumns();
            final NullableReadData[] datas = new NullableReadData[numColumns];
            int[] missingCols = new int[m_selectedColumns.length];
            int numMissing = 0;
            for (int i : m_selectedColumns) {
                final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(ReadDataReadCache.this, i, index);
                final NullableReadData cachedData = m_globalCache.getRetained(ccUID);
                if (cachedData != null) {
                    datas[i] = cachedData;
                } else {
                    missingCols[numMissing++] = i;
                }
            }
            if (numMissing > 0) {
                missingCols = Arrays.copyOf(missingCols, numMissing);

                // we use the first column's id as a proxy for locking.
                final ColumnDataUniqueId lockUID = new ColumnDataUniqueId(ReadDataReadCache.this, 0, index);
                final Object lock = m_cachedData.computeIfAbsent(lockUID, k -> new Object());
                synchronized (lock) {
                    try (RandomAccessBatchReader reader = m_readableDelegate
                            .createRandomAccessReader(new FilteredColumnSelection(numColumns, missingCols))) {
                        final ReadBatch batch = reader.readRetained(index);
                        for (int i : missingCols) {
                            final ColumnDataUniqueId ccUID = new ColumnDataUniqueId(ReadDataReadCache.this, i, index);
                            final NullableReadData data = batch.get(i);
                            final NullableReadData cachedData = m_globalCache.getRetained(ccUID);
                            if (cachedData != null) {
                                data.release();
                                datas[i] = cachedData;
                            } else {
                                m_cachedData.computeIfAbsent(ccUID, k -> new Object());
                                m_globalCache.put(ccUID, data, m_evictor);
                                datas[i] = data;
                            }
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException("Exception while loading column data.", e);
                    }
                }
            }

            return m_selection.createBatch(i -> datas[i]);
        }

        @Override
        public void close() throws IOException {
            // no resources held
        }

    }

    private final RandomAccessBatchReadable m_readableDelegate;

    private final EvictingCache<ColumnDataUniqueId, NullableReadData> m_globalCache;

    private Map<ColumnDataUniqueId, Object> m_cachedData = new ConcurrentHashMap<>();

    /**
     * @param readableDelegate the delegate from which to read in case of a cache miss
     * @param cache the cache for obtaining and storing data
     */
    public ReadDataReadCache(final RandomAccessBatchReadable readableDelegate, final SharedReadDataCache cache) {
        m_readableDelegate = readableDelegate;
        m_globalCache = cache.getCache();
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new ReadDataReadCacheReader(selection);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_readableDelegate.getSchema();
    }

    @Override
    public synchronized void close() throws IOException {
        if (m_cachedData != null) {
            for (final ColumnDataUniqueId id : m_cachedData.keySet()) {
                final NullableReadData removed = m_globalCache.removeRetained(id);
                if (removed != null) {
                    removed.release();
                }
            }
            m_cachedData.clear();
            m_cachedData = null;
            m_readableDelegate.close();
        }
    }

}
