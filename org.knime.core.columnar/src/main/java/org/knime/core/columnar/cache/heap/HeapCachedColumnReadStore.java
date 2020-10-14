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
package org.knime.core.columnar.cache.heap;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.ObjectData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

import com.google.common.cache.Cache;

/**
 * {@link ColumnReadStore} caching {@link ObjectData} as weak references in heap.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @since 4.3
 */
public final class HeapCachedColumnReadStore implements ColumnReadStore {

    private static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    private final class Reader implements ColumnDataReader {

        private final ColumnDataReader m_delegateReader;

        private final ColumnSelection m_selection;

        private boolean m_readerClosed;

        private Reader(final ColumnSelection selection) {
            m_delegateReader = m_delegate.createReader(selection);
            m_selection = selection;
        }

        @Override
        public ReadBatch readRetained(final int batchIndex) throws IOException {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            final ReadBatch batch = m_delegateReader.readRetained(batchIndex);
            return ColumnSelection.createBatch(m_selection,
                i -> m_objectData.isSelected(i) ? wrap(batch, batchIndex, i) : batch.get(i));
        }

        @SuppressWarnings("unchecked")
        private <T> HeapCachedReadData<T> wrap(final ReadBatch batch, final int batchIndex, final int columnIndex) {
            final ObjectReadData<T> columnReadData = (ObjectReadData<T>)batch.get(columnIndex);
            final AtomicReferenceArray<T> array = (AtomicReferenceArray<T>)m_cache.asMap().computeIfAbsent(
                new ColumnDataUniqueId(HeapCachedColumnReadStore.this, columnIndex, batchIndex),
                k -> new AtomicReferenceArray<>(columnReadData.length()));
            return new HeapCachedReadData<>(columnReadData, array);
        }

        @Override
        public int getNumBatches() throws IOException {
            return m_delegateReader.getNumBatches();
        }

        @Override
        public int getMaxLength() throws IOException {
            return m_delegateReader.getMaxLength();
        }

        @Override
        public void close() throws IOException {
            m_readerClosed = true;

            m_delegateReader.close();
        }

    }

    private final ColumnReadStore m_delegate;

    private final ColumnSelection m_objectData;

    private final Cache<ColumnDataUniqueId, AtomicReferenceArray<?>> m_cache;

    private volatile boolean m_storeClosed;

    /**
     * @param delegate the delegate from which to read
     * @param cache the delegate from which to read object data in case of a cache miss
     */
    public HeapCachedColumnReadStore(final ColumnReadStore delegate, final HeapCachedColumnStoreCache cache) {
        this(delegate, HeapCacheUtils.getObjectDataIndices(delegate.getSchema()), cache);
    }

    HeapCachedColumnReadStore(final ColumnReadStore delegate, final ColumnSelection selection,
        final HeapCachedColumnStoreCache cache) {
        m_delegate = delegate;
        m_objectData = selection;
        m_cache = cache.getCache();
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection selection) {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return new Reader(selection);
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public void close() throws IOException {
        m_storeClosed = true;

        m_delegate.close();
    }

}
