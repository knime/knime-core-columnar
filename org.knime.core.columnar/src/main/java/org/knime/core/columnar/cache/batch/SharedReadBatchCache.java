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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.filter.ColumnSelection;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

/**
 * Cache for {@link ReadBatch ReadBatches} that can be cleared from the outside.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class SharedReadBatchCache {

    private final Cache<BatchId, ReadBatch> m_cache;

    private final ReadWriteLock m_readWriteLock = new ReentrantReadWriteLock();

    private final long m_cacheSizeInBytes;

    /**
     * Constructor.
     *
     * @param cacheSizeInBytes the size of the cache in bytes
     */
    public SharedReadBatchCache(final long cacheSizeInBytes) {
        m_cacheSizeInBytes = cacheSizeInBytes;
        m_cache = CacheBuilder.newBuilder()//
                .removalListener(SharedReadBatchCache::evictBatch)//
                .weigher(SharedReadBatchCache::weighBatch)//
                .maximumWeight(cacheSizeInBytes)//
                .build();
    }

    /**
     * @return the maximal size of this cache in bytes
     */
    public long getMaxSizeInBytes() {
        return m_cacheSizeInBytes;
    }

    private static void evictBatch(final RemovalNotification<BatchId, ReadBatch> removal) {
        removal.getValue().release();
    }

    /**
     * @param id of the batch being added. Just present to match the {@link Weigher} signature
     */
    private static int weighBatch(final BatchId id, final ReadBatch batch) {
        var weight = batch.sizeOf();
        if (weight > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                "Size of data (%s) exceeds maximum weight (%s)".formatted(weight, Integer.MAX_VALUE));
        }
        return (int)weight;
    }

    /**
     * Releases all held batches and clears the cache.
     */
    public void clear() {
        // we have to wait until all getRetained calls are finished in order to avoid race conditions between
        // their retains and the releases of this call
        m_readWriteLock.writeLock().lock();
        try {
            m_cache.invalidateAll();
        } finally {
            m_readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Evicts all batches that originate from source. NOTE: It's the responsibility of the source to ensure that no
     * concurrent calls made that attempt to get batches that originate from the source.
     *
     * @param source for which to remove all batches that originated from it
     */
    void evictAllFromSource(final Object source) {
        m_cache.asMap().keySet().removeIf(id -> id.isFrom(source));
    }

    /**
     * @param id of the batch to retrieve
     * @param supplier called on cache miss, must return an already retained batch
     * @return the retained batch (i.e. the batch is guaranteed to not have been released)
     */
    ReadBatch getRetained(final BatchId id, final Loader<ReadBatch> supplier) throws IOException {
        return ensureNoClear(() -> getRetainedInternal(id, supplier));
    }

    /**
     * A Loader loads an object on cache miss.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param <T> the types of objects that are loaded
     */
    @FunctionalInterface
    public interface Loader<T> {

        /**
         * @return the loaded object
         * @throws IOException if the object can't be loaded
         */
        T load() throws IOException;
    }

    private ReadBatch getRetainedInternal(final BatchId id, final Loader<ReadBatch> loader) throws IOException {
        try {
            var batch = m_cache.get(id, loader::load);
            batch.retain();
            return batch;
        } catch (ExecutionException ex) {//NOSONAR ExecutionExceptions are just wrappers
            var cause = ex.getCause();
            if (cause instanceof IOException ioEx) {
                throw ioEx;
            }
            throw new IllegalStateException("Failed to load ReadBatch.", cause);
        }
    }

    Optional<ReadBatch> getRetained(final BatchId id) throws IOException {
        return ensureNoClear(() -> getRetainedInternal(id));
    }

    private Optional<ReadBatch> getRetainedInternal(final BatchId id) {
        var batch = Optional.ofNullable(m_cache.getIfPresent(id));
        batch.ifPresent(ReadBatch::retain);
        return batch;
    }

    private <T> T ensureNoClear(final Loader<T> loader) throws IOException {
        m_readWriteLock.readLock().lock();
        try {
            return loader.load();
        } finally {
            m_readWriteLock.readLock().unlock();
        }
    }

    void put(final BatchId id, final ReadBatch batch) {
        m_cache.put(id, batch);
    }

    static final class BatchId {

        private final int m_hashCode;

        private final Object m_source;

        private final int m_index;

        private final ColumnSelection m_selection;

        BatchId(final Object source, final ColumnSelection selection, final int index) {
            m_source = source;
            m_selection = selection;
            m_index = index;
            // TODO improve hashcode function
            m_hashCode = source.hashCode() + 13 * selection.hashCode() + 13 * index;
        }

        boolean isFrom(final Object source) {
            return m_source.equals(source);
        }

        @Override
        public int hashCode() {
            return m_hashCode;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            if (obj instanceof BatchId) {
                var batchId = (BatchId)obj;
                return m_index == batchId.m_index//
                    && m_source.equals(batchId.m_source)//
                    && m_selection.equals(batchId.m_selection);
            }
            return false;
        }

    }
}
