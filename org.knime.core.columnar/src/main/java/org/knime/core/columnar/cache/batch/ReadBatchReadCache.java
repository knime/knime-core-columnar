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
 *   Apr 5, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.batch;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.batch.SharedReadBatchCache.BatchId;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Caches the {@link ReadBatch ReadBatches} that it reads from another {@link RandomAccessBatchReadable} for fast read
 * access.<br>
 * <br>
 * Combines two caches:
 * <ul>
 * <li>A shared cache that retains all read batches and keeps track of their size in order to evict them when memory
 * gets sparse.
 * <li>A local cache with weak values that does not explicitly retain batches. Its main purpose is to lessen the load on
 * the shared cache which does a lot of book keeping.
 * </ul>
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ReadBatchReadCache implements RandomAccessBatchReadable {

    private final RandomAccessBatchReadable m_delegate;

    private final SharedReadBatchCache m_sharedCache;

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    private final int m_numBatches;

    /**
     * Constructs a new ReadBatchReadCache.
     *
     * @param delegate to read from on cache miss
     * @param cache to cache ReadBatches in
     * @param numBatches the number of batches in the {@link RandomAccessBatchReadable}
     */
    public ReadBatchReadCache(final RandomAccessBatchReadable delegate, final SharedReadBatchCache cache,
        final int numBatches) {
        m_delegate = delegate;
        m_sharedCache = cache;
        m_numBatches = numBatches;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public void close() throws IOException {
        if (!m_closed.getAndSet(true)) {
            m_sharedCache.evictAllFromSource(this);
            m_delegate.close();
        }
    }

    private void checkOpen() {
        if (m_closed.get()) {
            throw new IllegalStateException("The ReadBatchReadCache is already closed.");
        }
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        checkOpen();
        return new BatchCacheReader(selection);
    }

    private final class BatchCacheReader implements RandomAccessBatchReader {

        private final RandomAccessBatchReader m_source;

        private final ColumnSelection m_selection;

        private final LocalReadBatchCache m_localCache =
            new LocalReadBatchCache(this::getFromSharedCache, m_numBatches);

        BatchCacheReader(final ColumnSelection selection) {
            m_source = m_delegate.createRandomAccessReader(selection);
            m_selection = selection;
        }

        @Override
        public void close() throws IOException {
            // not removed from the shared cache because other subsequent readers might read
            // from the cache as well
            m_source.close();
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            checkOpen();
            return m_localCache.readRetained(index);
        }

        private ReadBatch getFromSharedCache(final int index) throws IOException {
            var id = new BatchId(ReadBatchReadCache.this, m_selection, index);
            return m_sharedCache.getRetained(id, () -> readFromSource(index));
        }

        private ReadBatch readFromSource(final int index) throws IOException {
            return m_source.readRetained(index);
        }

    }

}
