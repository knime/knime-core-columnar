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
 *   Oct 9, 2020 (dietzc): created
 */
package org.knime.core.columnar.cache.object;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.CachedData.CachedDataFactory;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.data.VarBinaryData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link RandomAccessBatchReadable} that for in-heap caching of {@link VarBinaryData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @since 4.3
 */
public final class ObjectReadCache implements RandomAccessBatchReadable {

    private final RandomAccessBatchReadable m_readableDelegate;

    private final CachedDataFactory[] m_cachedDataFactories;

    private final CacheManager m_cacheManager;

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    /**
     * @param delegate the delegate from which to read
     * @param cache the delegate from which to read object data in case of a cache miss
     */
    public ObjectReadCache(final RandomAccessBatchReadable delegate, final SharedObjectCache cache) {
        m_readableDelegate = delegate;
        m_cacheManager = new CacheManager(cache);
        m_cachedDataFactories =
            CachedDataFactoryBuilder.createForReading(m_cacheManager).build(delegate.getSchema());
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new ObjectReadCacheReader(selection);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_readableDelegate.getSchema();
    }

    @Override
    public synchronized void close() throws IOException {
        if (m_closed.getAndSet(true)) {
            return; // already closed!
        }
        m_cacheManager.close();
        m_readableDelegate.close();
    }

    private ColumnDataUniqueId createId(final int columnIndex, final int batchIndex) {
        return new ColumnDataUniqueId(this, columnIndex, batchIndex);
    }

    private final class ObjectReadCacheReader implements RandomAccessBatchReader {

        private final RandomAccessBatchReader m_readerDelegate;

        private ObjectReadCacheReader(final ColumnSelection selection) {
            m_readerDelegate = m_readableDelegate.createRandomAccessReader(selection);
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            final ReadBatch batch = m_readerDelegate.readRetained(index);
            return batch.decorate((i, d) -> m_cachedDataFactories[i].createReadData(d, createId(i, index)));
        }

        @Override
        public void close() throws IOException {
            m_readerDelegate.close();
        }

    }

    @Override
    public long[] getBatchBoundaries() {
        return m_readableDelegate.getBatchBoundaries();
    }
}
