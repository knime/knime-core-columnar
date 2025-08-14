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
 *   Sep 23, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.badger.BatchingWritable;
import org.knime.core.columnar.badger.HeapBadger;
import org.knime.core.columnar.badger.HeapCache;
import org.knime.core.columnar.badger.HeapCachingBatchWritable;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.cache.data.ReadDataCache;
import org.knime.core.columnar.cache.data.SharedReadDataCache;
import org.knime.core.columnar.cache.object.ObjectCache;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.cache.writable.BatchWritableCache;
import org.knime.core.columnar.cache.writable.SharedBatchWritableCache;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory.ColumnarWriteCursor;
import org.knime.core.columnar.data.dictencoding.DictEncodedBatchWritableReadable;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.data.columnar.domain.DomainWritable;
import org.knime.core.data.columnar.domain.DomainWritableConfig;
import org.knime.core.data.columnar.domain.DuplicateCheckWritable;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.util.memory.MemoryAlert;
import org.knime.core.data.util.memory.MemoryAlertListener;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.ThreadUtils;

/**
 * Enhances a {@link BatchStore} with additional features like caching, dictionary encoding, domain calculation and
 * duplicate checking as it is used in the columnar back-end.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DefaultColumnarBatchStore implements ColumnarBatchStore, BatchingWritable {
    /**
     * Builder pattern to wrap a plain {@link BatchWritable} and {@link RandomAccessBatchReader} in cache, dictionary
     * encoding and domain calculation layers.
     *
     * Call {@link ColumnarBatchStoreBuilder#build()} to obtain the final {@link WrappedBatchStore}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ColumnarBatchStoreBuilder {
        private final BatchWritable m_writeDelegate;

        private final BatchReadStore m_readDelegate;

        private SharedObjectCache m_heapCache = null;

        private ExecutorService m_heapCachePersistExecutor = null;

        private ExecutorService m_heapCacheSerializeExecutor = null;

        private SharedBatchWritableCache m_smallTableCache = null;

        private SharedReadDataCache m_columnDataCache = null;

        private ExecutorService m_columnDataCacheExecutor = null;

        private boolean m_dictEncodingEnabled = false;

        private boolean m_useHeapBadger = false;

        private ExecutorService m_duplicateCheckExecutor = null;

        private DomainWritableConfig m_domainCalculationConfig = null;

        private ExecutorService m_domainCalculationExecutor = null;

        /**
         * Create a {@link ColumnarBatchStoreBuilder} with given write and read delegates
         *
         * @param writable The {@link BatchWritable} delegate
         * @param readable The {@link RandomAccessBatchReadable} delegate
         */
        public ColumnarBatchStoreBuilder(final BatchWritable writable, final BatchReadStore readable) {
            m_writeDelegate = writable;
            m_readDelegate = readable;
        }

        /**
         * Create a {@link ColumnarBatchStoreBuilder} with a given read+write delegate
         *
         * @param delegate The delegate that acts as {@link BatchWritable} and {@link RandomAccessBatchReadable}
         */
        public <D extends BatchWritable & BatchReadStore> ColumnarBatchStoreBuilder(final D delegate) {
            m_writeDelegate = delegate;
            m_readDelegate = delegate;
        }

        /**
         * Enable heap caching of selected data columns (currently String and VarBinary).
         *
         * @param cache The heap cache to use for storing the data, or pass null to disable the heap cache
         * @param persistExec All tasks to store data in the heap cache will run on this executor
         * @param serializeExec Serialization to the underlying {@link BatchWriter} will be performed on this executor
         * @return This {@link ColumnarBatchStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchStoreBuilder useHeapCache(final SharedObjectCache cache, final ExecutorService persistExec,
            final ExecutorService serializeExec) {
            m_heapCache = cache;
            m_heapCachePersistExecutor = persistExec;
            m_heapCacheSerializeExecutor = serializeExec;
            return this;
        }

        /**
         * Use the heap badger for heap caching instead of the (old) heap cache
         *
         * @param cache The heap cache to use for storing the data, or pass null to disable the heap cache
         * @return This {@link ColumnarBatchStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchStoreBuilder useHeapBadger() {
            m_useHeapBadger = true;
            return this;
        }

        /**
         * Enable off-heap caching of small tables in the given cache or disable it by passing null.
         *
         * @param cache The cache to use for small tables
         * @return This {@link ColumnarBatchStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchStoreBuilder useSmallTableCache(final SharedBatchWritableCache cache) {
            m_smallTableCache = cache;
            return this;
        }

        /**
         * Enable off-heap caching of columnar data in the given cache, or disable it by passing null.
         *
         * @param cache The cache to use for columnar data.
         * @param exec The executor which is used to run serialization tasks in the column data cache
         * @return This {@link ColumnarBatchStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchStoreBuilder useColumnDataCache(final SharedReadDataCache cache,
            final ExecutorService exec) {
            m_columnDataCache = cache;
            m_columnDataCacheExecutor = exec;
            return this;
        }

        /**
         * Set dictionary encoding enabled
         *
         * @param enabled Pass true to enable dictionary encoding, false to disable it
         * @return This {@link ColumnarBatchStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchStoreBuilder enableDictEncoding(final boolean enabled) {
            m_dictEncodingEnabled = enabled;
            return this;
        }

        /**
         * Enable duplicate checking which runs on the given {@link ExecutorService}
         *
         * @param exec The executor on which to run duplicate checking tasks
         * @return This {@link ColumnarBatchStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchStoreBuilder useDuplicateChecking(final ExecutorService exec) {
            m_duplicateCheckExecutor = exec;
            return this;
        }

        /**
         * Enable domain calculation with the given configuration
         *
         * @param config The configuration for domain calculation
         * @param exec The executor to use for domain calculation tasks
         * @return This {@link ColumnarBatchStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchStoreBuilder useDomainCalculation(final DomainWritableConfig config,
            final ExecutorService exec) {
            m_domainCalculationConfig = config;
            m_domainCalculationExecutor = exec;
            return this;
        }

        /**
         * Build a {@link DefaultColumnarBatchStore} with the given configuration
         *
         * @return A new {@link DefaultColumnarBatchStore}
         */
        public DefaultColumnarBatchStore build() {
            return new DefaultColumnarBatchStore(this);
        }
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DefaultColumnarBatchStore.class);

    private BatchWritable m_writable;

    private RandomAccessBatchReadable m_readable;

    private ReadDataCache m_columnDataCache = null;

    private BatchWritableCache m_smallTableCache = null;

    private ObjectCache m_heapCache = null;

    private HeapBadger m_heapBadger = null;

    private MemoryAlertListener m_memListener = null;

    private DomainWritable m_domainWritable = null;

    private BatchSizeRecorder m_batchSizeRecorder = null;

    private final BatchReadStore m_readStore;

    private final ColumnarWriteCursor m_writeCursor;

    private DefaultColumnarBatchStore(final ColumnarBatchStoreBuilder builder) {
        m_readStore = builder.m_readDelegate;

        m_readable = builder.m_readDelegate;
        m_writable = builder.m_writeDelegate;

        initColumnDataCache(builder.m_columnDataCache, builder.m_columnDataCacheExecutor);

        initSmallTableCache(builder.m_smallTableCache);

        if (builder.m_dictEncodingEnabled) {
            final var dictEncoded = new DictEncodedBatchWritableReadable(m_writable, m_readable);
            m_readable = dictEncoded;
            m_writable = dictEncoded;
        }

        if (builder.m_useHeapBadger) {
            // In case of the heap badger, we put the duplicate check and domain calculation below, because they are designed to
            // work on batches at the moment. This might not be optimal, but is the quickest way to put everything together right now.
            // TODO: move duplicate check and domain calc before the HeapBadger by making them batch-unaware (again).
            initHeapCache(builder.m_heapCache);
            initDuplicateCheck(builder.m_duplicateCheckExecutor);
            initDomainCalculation(builder.m_domainCalculationConfig, builder.m_domainCalculationExecutor);
            m_writable = initBatchSizeRecorder(m_writable);
            initHeapBadger();
            m_writeCursor = m_heapBadger.getWriteCursor();
        } else {
            m_writable = initBatchSizeRecorder(m_writable);
            initObjectCache(builder.m_heapCache, builder.m_heapCachePersistExecutor,
                builder.m_heapCacheSerializeExecutor);
            initDuplicateCheck(builder.m_duplicateCheckExecutor);
            initDomainCalculation(builder.m_domainCalculationConfig, builder.m_domainCalculationExecutor);
            m_writeCursor = ColumnarWriteCursorFactory.createWriteCursor(m_writable);
        }

        m_readable = m_batchSizeRecorder.augment(m_readable);
    }

    private void initDuplicateCheck(final ExecutorService duplicateCheckExecutor) {
        if (duplicateCheckExecutor != null) {
            m_writable = new DuplicateCheckWritable(m_writable, new DuplicateChecker(), duplicateCheckExecutor);
        }
    }

    private void initDomainCalculation(final DomainWritableConfig config, final ExecutorService domainCalcExecutor) {
        if (config != null) {
            m_domainWritable = new DomainWritable(m_writable, config, domainCalcExecutor);
            m_writable = m_domainWritable;
        }
    }

    private BatchWritable initBatchSizeRecorder(final BatchWritable writable) {
        m_batchSizeRecorder = new BatchSizeRecorder();
        return m_batchSizeRecorder.wrap(writable);
    }

    private void initHeapCache(final SharedObjectCache cache) {
        if (cache != null) {
            var heapCache = new HeapCache(m_readable, cache);
            m_readable = heapCache;
            m_writable = new HeapCachingBatchWritable(m_writable, heapCache);
        }
    }

    private void initHeapBadger() {
        m_heapBadger = new HeapBadger(m_writable, ColumnarPreferenceUtils.getHeapBadgerSerializationExecutor());
        m_writable = null; // FIXME

        m_memListener = new MemoryAlertListener() {
            @Override
            @SuppressWarnings("resource") // WriteCursor will be closed by the ColumnarBatchStore
            protected boolean memoryAlert(final MemoryAlert alert) {
                new Thread(ThreadUtils.runnableWithContext(() -> {
                    try {
                        m_heapBadger.getWriteCursor().flush();
                    } catch (IOException ex) {
                        LOGGER.error("Error during enforced premature serialization of object data.", ex);
                    }
                })).start();
                return false;
            }
        };
        MemoryAlertSystem.getInstanceUncollected().addListener(m_memListener);
    }

    private void initObjectCache(final SharedObjectCache heapCache, final ExecutorService persistExec,
        final ExecutorService serializeExec) {
        if (heapCache == null || persistExec == null || serializeExec == null) {
            return;
        }

        m_heapCache = new ObjectCache(m_writable, m_readable, heapCache, persistExec, serializeExec);

        m_readable = m_heapCache;
        m_writable = m_heapCache;

        m_memListener = new MemoryAlertListener() {
            @Override
            protected boolean memoryAlert(final MemoryAlert alert) {
                new Thread(ThreadUtils.runnableWithContext(() -> {
                    try {
                        m_heapCache.flush();
                    } catch (IOException ex) {
                        LOGGER.error("Error during enforced premature serialization of object data.", ex);
                    }
                })).start();
                return false;
            }
        };
        MemoryAlertSystem.getInstanceUncollected().addListener(m_memListener);
    }

    private void initSmallTableCache(final SharedBatchWritableCache cache) {
        if (cache == null || cache.getCacheSize() == 0) {
            return;
        }

        m_smallTableCache = new BatchWritableCache(m_writable, m_readable, cache);
        m_readable = m_smallTableCache;
        m_writable = m_smallTableCache;
    }

    private void initColumnDataCache(final SharedReadDataCache cache, final ExecutorService exec) {
        if (cache == null || exec == null || cache.getMaxSizeInBytes() == 0) {
            return;
        }

        m_columnDataCache = new ReadDataCache(m_writable, m_readable, cache, exec);
        m_readable = m_columnDataCache;
        m_writable = m_columnDataCache;
    }

    @Override
    public BatchWriter getWriter() {
        if ( m_writable == null ) {
            throw new IllegalStateException(
                    "No writer available for the HeapBadger. Use the batching write cursor instead");
        }
        return m_writable.getWriter();
    }

    @Override
    public ColumnarWriteCursor getBatchingWriteCursor() {
        return m_writeCursor;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_readable.getSchema();
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return m_readable.createRandomAccessReader(selection);
    }

    @Override
    public void close() throws IOException {
        if (m_memListener != null) {
            MemoryAlertSystem.getInstanceUncollected().removeListener(m_memListener);
            m_memListener = null;
        }

        m_writeCursor.close();
        m_readable.close();
    }

    @SuppressWarnings("resource") // The heap badger write cursor is closed separately
    @Override
    public void flushObjects() throws IOException {
        if (m_heapCache != null) {
            m_heapCache.flush();
        }

        if (m_heapBadger != null) {
            m_heapBadger.getWriteCursor().flush();
        }
    }

    @Override
    public void flush() throws IOException {
        flushObjects();

        // The small table cache must be flushed after heap cache because if close was called on the writers,
        // the flush method of the heap cache writer will wait for the close method of the smallTableCache writer.
        // And due to a questionable assumption in the SmallTableCache, it does not write anything in flush() if
        // the writer was not called before. That could lead to empty batch stores being saved.
        if (m_smallTableCache != null) {
            m_smallTableCache.flush();
        }
        if (m_columnDataCache != null) {
            m_columnDataCache.flush();
        }
    }

    @Override
    public DomainWritable getDomainWritable() {
        return m_domainWritable;
    }

    @Override
    public long[] getBatchBoundaries() {
        return m_readable.getBatchBoundaries();
    }

    @Override
    public FileHandle getFileHandle() {
        return m_readStore.getFileHandle();
    }

    /**
     * Access to the delegate from the tests
     *
     * @return the {@link BatchWritable} delegate
     */
    @Override
    public BatchWritable getWritableDelegate() {
        return m_writable;
    }

    @Override
    public BatchReadStore getDelegateBatchReadStore() {
        return m_readStore;
    }
}
