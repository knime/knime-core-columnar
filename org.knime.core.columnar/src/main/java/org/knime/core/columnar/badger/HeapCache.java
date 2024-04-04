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
 *   21 Dec 2023 (chaubold): created
 */
package org.knime.core.columnar.badger;

import java.io.IOException;

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.CacheManager;
import org.knime.core.columnar.cache.object.CachedData.CachedDataFactory;
import org.knime.core.columnar.cache.object.CachedDataFactoryBuilder;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Holds the badger's secret stash of things
 *
 * @author chaubold
 */
public class HeapCache implements RandomAccessBatchReadable {

    private final RandomAccessBatchReadable m_delegate;

    private final ColumnarSchema m_schema;

    private final CacheManager m_cacheManager;

    private CachedDataFactory[] m_cachedDataFactories;

    public HeapCache(final RandomAccessBatchReadable delegate, final SharedObjectCache cache) {
        m_delegate = delegate;
        m_schema = delegate.getSchema();
        m_cacheManager = new CacheManager(cache);
        m_cachedDataFactories =
                CachedDataFactoryBuilder.createForReading(m_cacheManager).build(delegate.getSchema());
    }

    /**
     * package private because it is supposed to be called from the HeapBadger
     */
    void cacheData(final Object[] data, final ColumnDataUniqueId id) {
        m_cacheManager.cacheData(data, id);
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new HeapCacheReader(selection);
    }

    private ColumnDataUniqueId createId(final int columnIndex, final int batchIndex) {
        return new ColumnDataUniqueId(this, columnIndex, batchIndex);
    }

    private final class HeapCacheReader implements RandomAccessBatchReader {
        private RandomAccessBatchReader m_readerDelegate;

        private HeapCacheReader(final ColumnSelection selection) {
            m_readerDelegate = m_delegate.createRandomAccessReader(selection);
        }

        @Override
        public void close() throws IOException {
            m_readerDelegate.close();
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            // find as much as possible in cache, construct batch from things in cache and things read from readerDelegate?
            final ReadBatch batch = m_readerDelegate.readRetained(index);
            return batch.decorate((i, d) -> m_cachedDataFactories[i].createReadData(d, createId(i, index)));
        }

    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @Override
    public void close() throws IOException {
        m_cacheManager.close();
        m_delegate.close();
    }

    @Override
    public long[] getBatchBoundaries() {
        throw new IllegalStateException("The batch boundaries should be tracked outside of the cache");
    }
}
