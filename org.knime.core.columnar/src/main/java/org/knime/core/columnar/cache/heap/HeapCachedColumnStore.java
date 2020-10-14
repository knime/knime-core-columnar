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
 */
package org.knime.core.columnar.cache.heap;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

import com.google.common.cache.Cache;

/**
 * {@link ColumnStore} interception {@link ObjectReadData} for in-heap caching of objects.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @since 4.3
 */
public final class HeapCachedColumnStore implements ColumnStore {

    private static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    private final class Factory implements ColumnDataFactory {

        private final ColumnDataFactory m_delegateFactory;

        private Factory() {
            m_delegateFactory = m_delegate.getFactory();
        }

        @Override
        public WriteBatch create() {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            final WriteBatch batch = m_delegateFactory.create();
            final ColumnWriteData[] data = new ColumnWriteData[getSchema().getNumColumns()];

            for (int i = 0; i < data.length; i++) {
                if (m_objectData.isSelected(i)) {
                    final ObjectWriteData<?> columnWriteData = (ObjectWriteData<?>)batch.get(i);
                    data[i] = new HeapCachedWriteData<>(columnWriteData);
                } else {
                    data[i] = batch.get(i);
                }
            }
            return new DefaultWriteBatch(data, batch.capacity());
        }

    }

    private final class Writer implements ColumnDataWriter {

        private final ColumnDataWriter m_delegateWriter;

        private int m_numBatches;

        private Writer() {
            m_delegateWriter = m_delegate.getWriter();
        }

        @Override
        public void write(final ReadBatch batch) throws IOException {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            // TODO wait until flushed.

            final ColumnReadData[] data = new ColumnReadData[batch.getNumColumns()];
            for (int i = 0; i < data.length; i++) {
                if (m_objectData.isSelected(i)) {
                    final HeapCachedReadData<?> heapCachedData = (HeapCachedReadData<?>)batch.get(i);
                    m_cache.put(new ColumnDataUniqueId(m_readStore, i, m_numBatches), heapCachedData.getData());
                    data[i] = heapCachedData.getDelegate();
                } else {
                    data[i] = batch.get(i);
                }
            }
            m_numBatches++;
            m_delegateWriter.write(new DefaultReadBatch(data, batch.length()));
        }

        @Override
        public void close() throws IOException {
            m_writerClosed = true;

            m_delegateWriter.close();
        }

    }

    private final ExecutorService m_executor;

    private final ColumnStore m_delegate;

    private final HeapCachedColumnReadStore m_readStore;

    private final Factory m_factory;

    private final Writer m_writer;

    private final ColumnSelection m_objectData;

    private final Cache<ColumnDataUniqueId, AtomicReferenceArray<?>> m_cache;

    private volatile boolean m_writerClosed;

    private volatile boolean m_storeClosed;

    /**
     * @param delegate the delegate to which to write
     * @param cache the in-heap cache for storing object data
     * @param executor the executor to which to submit asynchronous serialization tasks
     */
    public HeapCachedColumnStore(final ColumnStore delegate, final HeapCachedColumnStoreCache cache,
        final ExecutorService executor) {
        m_delegate = delegate;
        m_objectData = HeapCacheUtils.getObjectDataIndices(delegate.getSchema());
        m_readStore = new HeapCachedColumnReadStore(delegate, cache);
        m_factory = new Factory();
        m_writer = new Writer();
        m_cache = cache.getCache();
        m_executor = executor;
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_readStore.getSchema();
    }

    @Override
    public ColumnDataFactory getFactory() {
        if (m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_factory;
    }

    @Override
    public ColumnDataWriter getWriter() {
        if (m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_writer;
    }

    @Override
    public void save(final File f) throws IOException {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        m_delegate.save(f);
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection config) {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }

        return m_readStore.createReader(config);
    }

    @Override
    public void close() throws IOException {
        m_storeClosed = true;

        m_writer.close();
        m_readStore.close();
        m_delegate.close();
    }

}
