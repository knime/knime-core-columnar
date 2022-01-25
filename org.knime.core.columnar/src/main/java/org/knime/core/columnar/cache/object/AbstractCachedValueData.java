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
 *   Dec 17, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.object;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.AbstractCachedData.AbstractCachedDataFactory;
import org.knime.core.columnar.cache.object.AbstractCachedData.AbstractCachedLoadingReadData;
import org.knime.core.columnar.cache.object.AbstractCachedData.AbstractCachedWriteData;
import org.knime.core.columnar.cache.object.CachedData.CachedValueReadData;
import org.knime.core.columnar.cache.object.CachedData.CachedWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains abstract implementations for CachedData implementations that cache actual Java objects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class AbstractCachedValueData {

    private AbstractCachedValueData() {

    }

    /**
     * Abstract implementation of a CachedDataFactory that does actual caching.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    abstract static class AbstractCachedValueDataFactory<T> extends AbstractCachedDataFactory {

        private final CacheManager m_cacheManager;

        protected final ExecutorService m_serializationExecutor;

        protected final CountUpDownLatch m_serializationLatch;


        AbstractCachedValueDataFactory(final ExecutorService executor, final Set<CachedWriteData> unclosedData,
            final CacheManager cacheManager, final ExecutorService serializationExecutor,
            final CountUpDownLatch serializationLatch) {
            super(executor, unclosedData);
            m_cacheManager = cacheManager;
            m_serializationExecutor = serializationExecutor;
            m_serializationLatch = serializationLatch;
        }

        @Override
        public final NullableReadData createReadData(final NullableReadData data, final ColumnDataUniqueId id) {
            @SuppressWarnings("unchecked")
            T array = (T)m_cacheManager.getOrCreate(id, () -> createArray(data.length()));
            return createCachedData(data, array);
        }

        @Override
        protected CachedWriteData createCachedData(final NullableWriteData data, final ColumnDataUniqueId id) {
            return createCachedData(data, d -> m_cacheManager.cacheData(d, id));
        }

        protected abstract CachedWriteData createCachedData(final NullableWriteData data,
            final Consumer<Object> cache);

        protected abstract NullableReadData createCachedData(final NullableReadData data, final T array);

        protected abstract T createArray(final int length);

    }

    /**
     * {@link AbstractCachedValueWriteData} wraps a delegate data object and performs the serialization asynchronously
     * using chained {@link CompletableFuture}s. When a {@link AbstractCachedValueWriteData} is closed, it creates a
     * {@link AbstractCachedValueReadData}, but continues serializing asynchronously if needed. Only when
     * closeWriteDelegate() is closed we block and wait for the serialization to finish.
     *
     * Call flush if you need all data written to the delegate.
     *
     * The interface of {@link AbstractCachedValueWriteData} is *not* thread safe, do not call set(), expand(), flush() and close()
     * concurrently.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    abstract static class AbstractCachedValueWriteData<W extends NullableWriteData, R extends NullableReadData, T>
        extends AbstractCachedWriteData<W, R> {

        private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCachedValueWriteData.class);

        private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for serialization thread.";

        // This parameter determines how many set operations must occur until a new serialization thread is launched.
        // It exists to prevent a new serialization thread to be launched after every set operation
        // (in case the serialization thread is faster than the thread producing the data and invoking set).
        private static final int SERIALIZATION_DELAY = 16;

        abstract class AbstractCachedValueReadData extends AbstractCachedReadData implements CachedValueReadData {

            AbstractCachedValueReadData(final int length) {
                super(length);
            }

            @Override
            public boolean isMissing(final int index) {
                return m_data[index] == null;
            }

            @Override
            public Object[] getData() {
                return m_data;
            }

            @Override
            public AbstractCachedValueWriteData<W, R, T> getWriteData() {
                return AbstractCachedValueWriteData.this;
            }

        }

        T[] m_data;

        private final ExecutorService m_serializationExecutor;

        // the index up to which data has been set
        private int m_setIndex = -1;

        // the index up to which data serialization has been dispatched
        private int m_highestDispatchedSerializationIndex = -1;

        // the serialization future
        private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

        private final AtomicBoolean m_cancelled = new AtomicBoolean(false);

        // used to make sure we don't call expand and a realloc during serialization concurrently.
        private final Lock m_expandLock = new ReentrantLock();

        // The latch is used to keep track of the number of asynchronously dispatched tasks to
        // know when they have really finished. This is used as a barrier to ensure we only close
        // the back-end once all writing has finished.
        private final CountUpDownLatch m_serializationLatch;

        private final Consumer<Object> m_cache;


        AbstractCachedValueWriteData(final W delegate, final T[] data, final ExecutorService executor,
            final CountUpDownLatch latch, final Consumer<Object> cache) {
            super(delegate);
            m_data = data;
            m_serializationExecutor = executor;
            m_serializationLatch = latch;
            m_cache = cache;
        }

        @Override
        public void setMissing(final int index) {
            // as per contract, setObject / setMissing is only ever called for ascending indices
            // m_data[index] is already null; no need to explicitly set it to null here
        }

        @Override
        public int capacity() {
            // we return m_data.length because that is always in sync with m_delegate.capacity()
            return m_data.length;
        }

        @Override
        public synchronized void retain() {
            m_future = m_future.thenRunAsync(m_delegate::retain, m_serializationExecutor);
        }

        @Override
        public synchronized void release() {
            m_future = m_future.thenRunAsync(m_delegate::release, m_serializationExecutor);
        }

        @Override
        public long sizeOf() {
            flush();
            return m_delegate.sizeOf();
        }

        @Override
        public void expand(final int minimumCapacity) {
            // We wait for all running serialization tasks to finish before expanding
            // the underlying buffer and then expand on the main thread to make sure
            // all data is written *and* the memory allocator of the delegate is not
            // invoked by multiple threads in parallel, but only from the thread calling expand.
            // To prevent serialization calling reAlloc of the underlying buffer in parallel,
            // we synchronize those operations on a lock.
            lockWriting();
            try {
                m_delegate.expand(minimumCapacity);
            } finally {
                unlockWriting();
            }

            expandCache();
        }

        @Override
        public void lockWriting() {
            waitForAndGetFuture();
            m_expandLock.lock();
        }

        @Override
        public void unlockWriting() {
            m_expandLock.unlock();
        }

        @Override
        public void expandCache() {
            m_data = Arrays.copyOf(m_data, m_delegate.capacity());
        }

        @Override
        public synchronized void flush() {
            if (m_future.isDone()) {
                waitForAndGetFuture();
            }
            dispatchSerialization();
            waitForAndGetFuture();
        }

        /**
         * Serialize a range of data elements from the m_data cache to the delegate
         * @param start first index to serialize, inclusive
         * @param end index one past the last index to serialize = exclusive upper bound
         */
        void serializeRange(final int start, final int end) {
            for (int i = start; i < Math.min(end, m_data.length); i++) {
                if (m_data[i] != null) {
                    if (m_cancelled.get()) {
                        break;
                    }
                    m_expandLock.lock();
                    try {
                        serializeAt(i);
                    } finally {
                        unlockWriting();
                    }
                }
            }

            m_serializationLatch.countDown();
        }

        /**
         * Dispatch the serialization of the remaining unsaved items
         */
        synchronized void dispatchSerialization() {
            if (m_future.isDone()) {
                waitForAndGetFuture();
            }

            if (m_cancelled.get()) {
                return;
            }

            final var highest = m_setIndex;
            final var lowest = m_highestDispatchedSerializationIndex;

            m_serializationLatch.countUp();
            m_future = m_future.thenRunAsync(() -> serializeRange(lowest + 1, highest + 1), m_serializationExecutor);
            m_highestDispatchedSerializationIndex = highest;
        }

        abstract void serializeAt(final int index);

        void onSet(final int index) {
            m_setIndex = index;

            if (m_setIndex > m_highestDispatchedSerializationIndex + SERIALIZATION_DELAY) {
                dispatchSerialization();
            }
        }

        synchronized void onClose() {
            m_cache.accept(m_data);
            dispatchSerialization();
        }

        @Override
        public synchronized void waitForAndGetFuture() {
            try {
                m_future.get();
            } catch (ExecutionException e) {
                throw new IllegalStateException("Failed to asynchronously serialize object data.", e);
            } catch (InterruptedException e) {
                // Restore interrupted state
                Thread.currentThread().interrupt();
                LOGGER.info(ERROR_ON_INTERRUPT, e);
            }
        }

        @Override
        public void cancel() {
            m_cancelled.set(true);
            waitForAndGetFuture();
        }

        @Override
        public abstract R closeDelegate(int length);

    }


    /**
     * Abstract class for CachedLoadingReadData implementations that hold actual values.
     * Manages the reference counting of the value array.
     *
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    abstract static class AbstractCachedLoadingValueReadData<R extends NullableReadData, T>
        extends AbstractCachedLoadingReadData<R> {

        private final AtomicInteger m_refCounter = new AtomicInteger(1);

        T[] m_data;

        AbstractCachedLoadingValueReadData(final R delegate, final T[] data) {
            super(delegate);
            m_data = data;
        }

        @Override
        public void retainCache() {
            m_refCounter.getAndIncrement();
        }

        @Override
        public void releaseCache() {
            if (m_refCounter.decrementAndGet() == 0) {
                m_data = null;
            }
        }

    }
}
