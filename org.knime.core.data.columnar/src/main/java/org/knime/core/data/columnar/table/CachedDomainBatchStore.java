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
 *   17 Nov 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.data.ReadDataCache;
import org.knime.core.columnar.cache.data.SharedReadDataCache;
import org.knime.core.columnar.cache.object.ObjectCache;
import org.knime.core.columnar.cache.object.SharedObjectCache;
import org.knime.core.columnar.cache.writable.BatchWritableCache;
import org.knime.core.columnar.cache.writable.SharedBatchWritableCache;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.columnar.domain.DefaultDomainWritableConfig;
import org.knime.core.data.columnar.domain.DomainWritable;
import org.knime.core.data.columnar.domain.DuplicateCheckWritable;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.CachedBatchReadStore.WrappedRandomAccessBatchReader;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.util.memory.MemoryAlert;
import org.knime.core.data.util.memory.MemoryAlertListener;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.util.DuplicateChecker;

/**
 * A {@link BatchStore} that delegates operations through
 * <ul>
 * <li>A {@link DomainWritable},</li>
 * <li>a {@link DuplicateCheckWritable},</li>
 * <li>an {@link ObjectCache},</li>
 * <li>a {@link BatchWritableCache}, and</li>
 * <li>a {@link ReadDataCache}.</li>
 * </ul>
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class CachedDomainBatchStore implements BatchStore, Flushable {

    private static final class WrappedBatchWriter implements BatchWriter {

        private final BatchWritable m_delegateWritable;

        private final AtomicBoolean m_storeClosed;

        private final AtomicBoolean m_writerClosed;

        private final AtomicInteger m_numBatches = new AtomicInteger();

        private final AtomicInteger m_batchLength = new AtomicInteger();

        private BatchWriter m_delegateWriter;

        WrappedBatchWriter(final BatchWritable wrappedDelegateWritable, final AtomicBoolean storeClosed,
            final AtomicBoolean writerClosed) {
            m_delegateWritable = wrappedDelegateWritable;
            m_storeClosed = storeClosed;
            m_writerClosed = writerClosed;
        }

        @SuppressWarnings("resource")
        @Override
        public final WriteBatch create(final int capacity) {
            if (m_writerClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            return initAndGetDelegate().create(capacity);
        }

        @SuppressWarnings("resource")
        @Override
        public final void write(final ReadBatch batch) throws IOException {
            if (m_storeClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }
            if (m_writerClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }

            if (m_numBatches.incrementAndGet() == 1) {
                m_batchLength.set(batch.length());
            }
            initAndGetDelegate().write(batch);
        }

        @Override
        public final void close() throws IOException {
            if (!m_writerClosed.getAndSet(true) && m_delegateWriter != null) {
                m_delegateWriter.close();
            }
        }

        private final BatchWriter initAndGetDelegate() {
            if (m_delegateWriter == null) {
                m_delegateWriter = m_delegateWritable.getWriter();
            }
            return m_delegateWriter;
        }

    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(CachedDomainBatchStore.class);

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

    private final BatchStore m_unwrappedDelegateStore;

    private final BatchWritableCache m_smallCached;

    private final ReadDataCache m_dataCached;

    private final ObjectCache m_objectCached;

    private final MemoryAlertListener m_memListener;

    private final DomainWritable m_domainCalculated;

    private final BatchWritable m_wrappedDelegateWritable;

    private final RandomAccessBatchReadable m_wrappedDelegateReadable;

    private final AtomicBoolean m_storeClosed = new AtomicBoolean();

    private final AtomicBoolean m_writerClosed = new AtomicBoolean();

    private final WrappedBatchWriter m_writer;

    CachedDomainBatchStore(final BatchStore store, final ColumnarValueSchema schema,
        final ColumnarRowContainerSettings settings) {
        m_unwrappedDelegateStore = store;

        final SharedReadDataCache columnDataCache = ColumnarPreferenceUtils.getColumnDataCache();
        final SharedBatchWritableCache smallTableCache = ColumnarPreferenceUtils.getSmallTableCache();

        if (columnDataCache.getMaxSizeInBytes() > 0) {
            m_dataCached = new ReadDataCache(store, columnDataCache, ColumnarPreferenceUtils.getPersistExecutor());
        } else {
            m_dataCached = null;
        }

        if (smallTableCache.getCacheSize() > 0) {
            if (m_dataCached != null) {
                m_smallCached = new BatchWritableCache(m_dataCached, smallTableCache);
            } else {
                m_smallCached = new BatchWritableCache(store, smallTableCache);
            }
        } else {
            m_smallCached = null;
        }

        final SharedObjectCache heapCache = ColumnarPreferenceUtils.getHeapCache();
        final ExecutorService executor = ColumnarPreferenceUtils.getSerializeExecutor();
        if (m_smallCached != null) {
            m_objectCached = new ObjectCache(m_smallCached, heapCache, executor);
        } else if (m_dataCached != null) {
            m_objectCached = new ObjectCache(m_dataCached, heapCache, executor);
        } else {
            m_objectCached = new ObjectCache(store, heapCache, executor);
        }
        m_memListener = new MemoryAlertListener() {
            @Override
            protected boolean memoryAlert(final MemoryAlert alert) {
                new Thread(() -> {
                    try {
                        m_objectCached.flush();
                    } catch (IOException ex) {
                        LOGGER.error("Error during enforced premature serialization of object data.", ex);
                    }
                }).start();
                return false;
            }
        };
        MemoryAlertSystem.getInstanceUncollected().addListener(m_memListener);

        BatchWritable wrappedWritable = m_objectCached;
        if (settings.isCheckDuplicateRowKeys()) {
            wrappedWritable = new DuplicateCheckWritable(wrappedWritable, new DuplicateChecker(),
                ColumnarPreferenceUtils.getDuplicateCheckExecutor());
        }

        m_domainCalculated = new DomainWritable(wrappedWritable, new DefaultDomainWritableConfig(schema,
            settings.getMaxPossibleNominalDomainValues(), settings.isInitializeDomains()),
            ColumnarPreferenceUtils.getDomainCalcExecutor());
        wrappedWritable = m_domainCalculated;

        m_wrappedDelegateWritable = wrappedWritable;
        m_wrappedDelegateReadable = m_objectCached;

        m_writer = new WrappedBatchWriter(m_wrappedDelegateWritable, m_storeClosed, m_writerClosed);
    }

    @Override
    public final ColumnarSchema getSchema() {
        return m_unwrappedDelegateStore.getSchema();
    }

    @Override
    public int numBatches() {
        return m_writer.m_numBatches.get();
    }

    @Override
    public int batchLength() {
        return m_writer.m_batchLength.get();
    }

    @Override
    public final BatchWriter getWriter() {
        if (m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_writer;
    }

    @Override
    public final void flush() throws IOException {
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        if (m_smallCached != null) {
            m_smallCached.flush();
        }
        if (m_dataCached != null) {
            m_dataCached.flush();
        }
        m_objectCached.flush();
    }

    final DataColumnDomain getDomain(final int colIndex) {
        return m_domainCalculated.getDomain(colIndex);
    }

    final DataColumnMetaData[] getMetadata(final int colIndex) {
        return m_domainCalculated.getMetadata(colIndex);
    }

    void setMaxPossibleValues(final int maxPossibleValues) {
        m_domainCalculated.setMaxPossibleValues(maxPossibleValues);
    }

    @Override
    public final RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        if (!m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return new WrappedRandomAccessBatchReader(m_wrappedDelegateReadable, selection, m_storeClosed, numBatches());
    }

    @Override
    public final void close() throws IOException {
        if (!m_storeClosed.getAndSet(true)) {
            m_domainCalculated.close();
            m_writer.close();
            m_wrappedDelegateReadable.close();
            MemoryAlertSystem.getInstanceUncollected().removeListener(m_memListener);
        }
    }

}
