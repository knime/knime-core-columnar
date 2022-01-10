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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.CachedData.CachedDataFactory;
import org.knime.core.columnar.cache.object.CachedData.CachedLoadingReadData;
import org.knime.core.columnar.cache.object.CachedData.CachedReadData;
import org.knime.core.columnar.cache.object.CachedData.CachedWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Contains abstract implementation for object caching.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class AbstractCachedData {

    private AbstractCachedData() {

    }

    abstract static class AbstractCachedDataFactory implements CachedDataFactory {

        private final ExecutorService m_persistExecutor;

        private final Set<CachedWriteData> m_unclosedData;

        AbstractCachedDataFactory(final ExecutorService persistExecutor, final Set<CachedWriteData> unclosedData) {
            m_persistExecutor = persistExecutor;
            m_unclosedData = unclosedData;
        }

        @Override
        public CompletableFuture<NullableReadData> getCachedDataFuture(final NullableReadData data) {
            final var heapCachedData = (CachedReadData)data;
            return CompletableFuture.supplyAsync(() -> {
                m_unclosedData.remove(heapCachedData.getWriteData());
                return heapCachedData.closeWriteDelegate();
            }, m_persistExecutor);
        }

        @Override
        public final NullableWriteData createWriteData(final NullableWriteData data, final ColumnDataUniqueId id) {
            var cachedData = createCachedData(data, id);
            if (id.isColumnLevel()) {
                m_unclosedData.add(cachedData);
            }
            return cachedData;
        }

        protected abstract CachedWriteData createCachedData(final NullableWriteData data, ColumnDataUniqueId id);
    }

    /**
     * Abstract implementation of a CachedWriteData object that either holds values itself or contains other
     * CachedWriteData objects.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    abstract static class AbstractCachedWriteData<W extends NullableWriteData, R extends NullableReadData>
        implements CachedWriteData {

        protected final W m_delegate;

        AbstractCachedWriteData(final W writeDelegate) {
            m_delegate = writeDelegate;
        }

        @SuppressWarnings("unchecked")
        protected R closeDelegate(final int length) {
            return (R)m_delegate.close(length);
        }

        @Override
        public int capacity() {
            return m_delegate.capacity();
        }

        @Override
        public void retain() {
            m_delegate.retain();
        }

        @Override
        public void release() {
            m_delegate.release();
        }

        @Override
        public long sizeOf() {
            flush();
            return m_delegate.sizeOf();
        }

        abstract class AbstractCachedReadData implements CachedReadData {

            private final int m_length;

            private int m_numRetainsPriorSerialize = 0;

            protected R m_readDelegate;

            AbstractCachedReadData(final int length) {
                m_length = length;
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
                        AbstractCachedWriteData.this.release();
                    } else {
                        m_numRetainsPriorSerialize--;
                    }
                }
            }

            @Override
            public synchronized long sizeOf() {
                return m_readDelegate != null ? m_readDelegate.sizeOf() : AbstractCachedWriteData.this.sizeOf();
            }

            // MUST BE CALLED! (If on column level, otherwise the read delegate is set by the parent data)
            @Override
            public synchronized R closeWriteDelegate() {
                // we don't need to dispatch any more serialization here, that has happened in
                // {@link CachedWriteData} onClose() already. But we need to wait for this serialization
                // to finish.
                AbstractCachedWriteData.this.waitForAndGetFuture();

                m_readDelegate = AbstractCachedWriteData.this.closeDelegate(m_length);
                for (int i = 0; i < m_numRetainsPriorSerialize; i++) {//NOSONAR
                    m_readDelegate.retain();
                }

                return m_readDelegate;
            }

            @Override
            public CachedWriteData getWriteData() {
                return AbstractCachedWriteData.this;
            }

            @SuppressWarnings("unchecked")
            @Override
            public synchronized void setReadDelegate(final NullableReadData readDelegate) {
                // TODO do we also need to do the retainment stuff? Or is it enough if that happens on column level?
                m_readDelegate = (R)readDelegate;
            }

        }

    }

    /**
     * Abstract base class for all CachedLoadingReadData implementations.
     * Handles delegation to the underlying {@link NullableReadData} and provides implementation of high-level API.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    abstract static class AbstractCachedLoadingReadData<R extends NullableReadData> implements CachedLoadingReadData {

        protected final R m_delegate;

        AbstractCachedLoadingReadData(final R delegate) {
            m_delegate = delegate;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_delegate.isMissing(index);
        }

        @Override
        public int length() {
            return m_delegate.length();
        }

        @Override
        public void retain() {
            retainCache();
            m_delegate.retain();
        }

        @Override
        public void release() {
            releaseCache();
            m_delegate.release();
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
        }

    }

}
