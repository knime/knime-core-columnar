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

import static org.knime.core.columnar.filter.ColumnSelection.getSelectedColumns;

import java.io.IOException;
import java.util.Arrays;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.data.SharedReadDataCache.ColumnDataKey;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link RandomAccessBatchReadable} that reads {@link ReadData} from a delegate and places it in a fixed-size
 * {@link SharedReadDataCache LRU cache} in memory for faster subsequent access.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Tobias Pietzsch
 */
public sealed class ReadDataReadCache implements RandomAccessBatchReadable permits ReadDataCache {

    sealed class ReadDataReadCacheReader implements RandomAccessBatchReader permits ReadDataCache.ReadDataCacheReader {

        private final ColumnSelection m_selection;

        private final int[] m_selectedColumns;

        ReadDataReadCacheReader(final ColumnSelection selection) {
            m_selection = selection;
            m_selectedColumns = getSelectedColumns(selection);
        }

        /**
         * Try to fill the {@code datas} at the given {@code cols} indices from {@code m_globalCache}.
         * The indices of columns that could not be filled are returned as an {@code int[]} array.
         *
         * @param datas array of datas to populate
         * @param batch the index of the batch
         * @param cols the column indices (in datas) to populate
         * @return the column indices that could not be populated
         */
        private int[] populateFromCache(final NullableReadData[] datas, final int batch, final int[] cols) {
            final int[] missingCols = new int[cols.length];
            int numMissing = 0;
            for (int i : cols) {
                final ColumnDataKey ccUID = new ColumnDataKey(m_ownerHandle, i, batch);
                final NullableReadData cachedData = m_globalCache.getRetained(ccUID);
                if (cachedData != null) {
                    datas[i] = cachedData;
                } else {
                    missingCols[numMissing++] = i;
                }
            }
            return Arrays.copyOf(missingCols, numMissing);
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            final int numColumns = m_selection.numColumns();
            final NullableReadData[] datas = new NullableReadData[numColumns];
            int[] missingCols = populateFromCache(datas, index, m_selectedColumns);
            if (missingCols.length != 0) {

                // no two threads should read the same batch (index) concurrently
                synchronized (m_locks.get(index)) {

                    // NB: We might have been waiting for the lock, while some
                    // other thread was reading the batch. That thread may have
                    // put (some or all of) our missing columns in the cache
                    // already. So, we first try to get the missing columns from
                    // the cache again. Maybe we have to read fewer columns or
                    // nothing at all.
                    missingCols = populateFromCache(datas, index, missingCols);
                    if (missingCols.length != 0) {
                        try (RandomAccessBatchReader reader = m_readableDelegate
                                .createRandomAccessReader(new FilteredColumnSelection(numColumns, missingCols))) {
                            final ReadBatch batch = reader.readRetained(index);
                            for (int i : missingCols) {
                                final ColumnDataKey ccUID = new ColumnDataKey(m_ownerHandle, i, index);
                                final NullableReadData data = batch.get(i);
                                put(ccUID, data); // retains for the cache
                                data.retain(); // retain for the ReadBatch we will construct
                                datas[i] = data;
                            }
                            batch.release();
                        } catch (IOException e) {
                            throw new IllegalStateException("Exception while loading column data.", e);
                        }
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

    private final SharedReadDataCache m_globalCache;

    final SharedReadDataCache.DataOwner m_ownerHandle;

    /**
     * Provides indexed {@code Object} monitors to synchronize on.
     * <p>
     * We use this to prevent concurrent reading of the same batch from the delegate reader (while allowing concurrent
     * reading of different batches).
     */
    private static class Locks {

        private final WeakHashMap<Integer, Object> m_objects = new WeakHashMap<>();

        synchronized Object get(final int i) {
            return m_objects.computeIfAbsent(i, k -> new Object());
        }
    }

    private final Locks m_locks = new Locks();

    /**
     * @param readableDelegate the delegate from which to read in case of a cache miss
     * @param cache the cache for obtaining and storing data
     */
    public ReadDataReadCache(final RandomAccessBatchReadable readableDelegate, final SharedReadDataCache cache) {
        m_readableDelegate = readableDelegate;
        m_globalCache = cache;
        m_ownerHandle = new SharedReadDataCache.DataOwner();
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new ReadDataReadCacheReader(selection);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_readableDelegate.getSchema();
    }

    /**
     * Put the given {@code value} into the (local and global) cache with the given {@code key}.
     */
    protected void put(final ColumnDataKey key, final NullableReadData value) {
        m_globalCache.put(key, value);
    }

    final AtomicBoolean m_closed = new AtomicBoolean(false);

    @Override
    public synchronized void close() throws IOException {
        if (!m_closed.getAndSet(true)) {
            _close();
        }
    }

    /**
     * This method is called (once) on the first invocation of {@link #close()}.
     * <p>
     * This is overridden in {@link ReadDataCache} to do additional clean-up.
     */
    protected void _close() throws IOException {
        // Drop all globally cached data referenced by this cache
        m_globalCache.invalidateAll(m_ownerHandle);
        m_readableDelegate.close();
    }

    @Override
    public long[] getBatchBoundaries() {
        return m_readableDelegate.getBatchBoundaries();
    }
}
