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

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.cache.data.ReadDataReadCache;
import org.knime.core.columnar.cache.data.SharedReadDataCache;
import org.knime.core.columnar.cache.object.ObjectReadCache;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.data.dictencoding.DictEncodedBatchReadable;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Enhances a {@link BatchReadStore} with additional features like caching and dictionary encoding.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DefaultColumnarBatchReadStore implements ColumnarBatchReadStore {
    /**
     * Builder pattern to wrap a plain {@link RandomAccessBatchReader} in cache and dictionary encoding layers.
     *
     * Call {@link ColumnarBatchReadStoreBuilder#build()} to obtain the final {@link WrappedBatchStore}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ColumnarBatchReadStoreBuilder {
        private final BatchReadStore m_readStore;

        private SharedObjectCache m_heapCache = null;

        private SharedReadDataCache m_columnDataCache = null;

        private boolean m_dictEncodingEnabled = false;

        /**
         * Create a {@link ColumnarBatchReadStoreBuilder} with given write and read delegates
         *
         * @param readStore The {@link BatchReadStore} delegate
         */
        public ColumnarBatchReadStoreBuilder(final BatchReadStore readStore) {
            m_readStore = readStore;
        }

        /**
         * Try to read data from the given heap cache before asking the delegate.
         *
         * @param cache The heap cache to use for reading the data, or pass null to disable the heap cache
         * @return This {@link ColumnarBatchReadStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchReadStoreBuilder useHeapCache(final SharedObjectCache cache) {
            m_heapCache = cache;
            return this;
        }

        /**
         * Enable reading columnar data from the given off-heap cache, or disable it by passing null.
         *
         * @param cache The cache to use when reading columnar data.
         * @return This {@link ColumnarBatchReadStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchReadStoreBuilder useColumnDataCache(final SharedReadDataCache cache) {
            m_columnDataCache = cache;
            return this;
        }


        /**
         * Set dictionary encoding enabled
         *
         * @param enabled Pass true to enable dictionary encoding, false to disable it
         * @return This {@link ColumnarBatchReadStoreBuilder} to continue configuring it.
         */
        public ColumnarBatchReadStoreBuilder enableDictEncoding(final boolean enabled) {
            m_dictEncodingEnabled = enabled;
            return this;
        }

        /**
         * Build a {@link DefaultColumnarBatchReadStore} with the given configuration
         *
         * @return A new {@link DefaultColumnarBatchReadStore}
         */
        public DefaultColumnarBatchReadStore build() {
            return new DefaultColumnarBatchReadStore(this);
        }
    }

    private final RandomAccessBatchReadable m_readable;

    private final BatchReadStore m_delegateBatchReadStore;

    @SuppressWarnings("resource")
    private DefaultColumnarBatchReadStore(final ColumnarBatchReadStoreBuilder builder) {
        m_delegateBatchReadStore = builder.m_readStore;

        RandomAccessBatchReadable readable = builder.m_readStore;
        readable = initColumnDataCache(readable, builder.m_columnDataCache);
        readable = initDictEncoding(readable, builder.m_dictEncodingEnabled);
        readable = initHeapCache(readable, builder.m_heapCache);
        m_readable = readable;
    }

    private static RandomAccessBatchReadable initHeapCache(final RandomAccessBatchReadable readable,
        final SharedObjectCache heapCache) {
        return (heapCache == null) ? readable : new ObjectReadCache(readable, heapCache);
    }

    private static RandomAccessBatchReadable initColumnDataCache(final RandomAccessBatchReadable readable,
        final SharedReadDataCache cache) {
        return (cache == null || cache.getMaxSizeInBytes() == 0) ? readable : new ReadDataReadCache(readable, cache);
    }

    @SuppressWarnings("resource")
    private static RandomAccessBatchReadable initDictEncoding(final RandomAccessBatchReadable readable,
        final boolean dictEncodingEnabled) {
        return dictEncodingEnabled ? new DictEncodedBatchReadable(readable) : readable;
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
        m_readable.close();
    }

    @Override
    public int numBatches() {
        return m_delegateBatchReadStore.numBatches();
    }

    @Override
    public long[] getBatchBoundaries() {
        return m_delegateBatchReadStore.getBatchBoundaries();
    }

    @Override
    public long numRows() {
        return m_delegateBatchReadStore.numRows();
    }

    @Override
    public FileHandle getFileHandle() {
        return m_delegateBatchReadStore.getFileHandle();
    }

    /**
     * Access to the delegate from the tests
     * @return the readable delegate
     */
    RandomAccessBatchReadable getDelegate() {
        return m_readable;
    }

    @Override
    public BatchReadStore getDelegateBatchReadStore() {
        return m_delegateBatchReadStore;
    }
}
