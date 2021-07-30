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
 *   28 May 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.cache.object;

import java.io.Flushable;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CachedWriteData} wraps a delegate data object and performs the serialization asynchronously
 * using chained {@link CompletableFuture}s. When a {@link CachedWriteData} is closed, it creates a
 * {@link CachedReadData}, but continues serializing asynchronously if needed. Only when
 * closeWriteDelegate() is closed we block and wait for the serialization to finish.
 *
 * Call flush if you need all data written to the delegate.
 *
 * The interface of {@link CachedWriteData} is *not* thread safe, do not call set(), expand(), flush() and close()
 * concurrently.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
abstract class CachedWriteData<W extends NullableWriteData, R extends NullableReadData, T>
    implements NullableWriteData, Flushable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachedWriteData.class);

    private static final String ERROR_ON_INTERRUPT = "Interrupted while waiting for serialization thread.";

    // This parameter determines how many set operations must occur until a new serialization thread is launched.
    // It exists to prevent a new serialization thread to be launched after every set operation
    // (in case the serialization thread is faster than the thread producing the data and invoking set).
    private static final int SERIALIZATION_DELAY = 16;

    abstract class CachedReadData implements NullableReadData {

        private R m_readDelegate;

        private final int m_length;

        private int m_numRetainsPriorSerialize = 0;

        CachedReadData(final int length) {
            m_length = length;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_data[index] == null;
        }

        @Override
        public int length() {
            return m_length;
        }

        @Override
        public synchronized void retain() {
            if (m_readDelegate != null) {
                m_readDelegate.retain();
            } else {
                m_numRetainsPriorSerialize++;
            }
        }

        @Override
        public synchronized void release() {
            if (m_readDelegate != null) {
                m_readDelegate.release();
            } else {
                if (m_numRetainsPriorSerialize == 0) {
                    // as per contract, the HeapCachedWriteData's ref count was at 1 when it was closed
                    // therefore, the reference count would drop to 0 here and we have to release the HeapCachedWriteData
                    // this means that a subsequent call to serialize should and will fail
                    CachedWriteData.this.release();
                } else {
                    m_numRetainsPriorSerialize--;
                }
            }
        }

        @Override
        public synchronized long sizeOf() {
            return m_readDelegate != null ? m_readDelegate.sizeOf() : CachedWriteData.this.sizeOf();
        }

        Object[] getData() {
            return m_data;
        }

        // MUST BE CALLED!
        synchronized R closeWriteDelegate() {

            CachedWriteData.this.waitForAndGetFuture();

            m_readDelegate = CachedWriteData.this.closeDelegate(m_length);
            if (m_numRetainsPriorSerialize > 0) {
                for (int i = 0; i < m_numRetainsPriorSerialize; i++) {
                    m_readDelegate.retain();
                }
            }

            return m_readDelegate;
        }

        CachedWriteData<W, R, T> getWriteData() {
            return CachedWriteData.this;
        }

    }

    final W m_delegate;

    T[] m_data;

    private final ExecutorService m_executor;

    // the index up to which data has been set
    private int m_setIndex = -1;

    // the index up to which data serialization has been dispatched
    private int m_highestDispatchedSerializationIndex = -1;

    // the serialization future
    private CompletableFuture<Void> m_future = CompletableFuture.completedFuture(null);

    // remember whether this data has been closed already, to prevent double close
    private boolean m_closed = false;

    CachedWriteData(final W delegate, final T[] data, final ExecutorService executor) {
        m_delegate = delegate;
        m_data = data;
        m_executor = executor;
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
    public void retain() {
        m_future = m_future.thenRunAsync(m_delegate::retain, m_executor);
    }

    @Override
    public void release() {
        m_future = m_future.thenRunAsync(m_delegate::release, m_executor);
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
        waitForAndGetFuture();
        m_delegate.expand(minimumCapacity);
        m_data = Arrays.copyOf(m_data, m_delegate.capacity());
    }

    @Override
    public void flush() {
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
                serializeAt(i);
            }
        }
    }

    /**
     * Dispatch the serialization of the remaining unsaved items
     */
    void dispatchSerialization() {
        if (m_future.isDone()) {
            waitForAndGetFuture();
        }

        final var highest = m_setIndex;
        final var lowest = m_highestDispatchedSerializationIndex;

        m_future = m_future.thenRunAsync(() -> serializeRange(lowest + 1, highest + 1), m_executor);
        m_highestDispatchedSerializationIndex = highest;
    }

    abstract void serializeAt(final int index);

    void onSet(final int index) {
        m_setIndex = index;

        if (m_setIndex > m_highestDispatchedSerializationIndex + SERIALIZATION_DELAY) {
            dispatchSerialization();
        }
    }

    void onClose() {
        if (m_closed) {
            throw new IllegalStateException("Failed to close already closed CachedWriteData");
        }
        m_closed = true;

        dispatchSerialization();
    }

    private synchronized void waitForAndGetFuture() {
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

    abstract R closeDelegate(int length);

}
