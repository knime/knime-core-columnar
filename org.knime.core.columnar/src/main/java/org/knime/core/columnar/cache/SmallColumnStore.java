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
 */
package org.knime.core.columnar.cache;

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_READER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.File;
import java.io.IOException;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnStore} that stores {@link ColumnData} in a fixed-size {@link SmallColumnStoreCache LRU cache} in
 * memory if the aggregated {@link ReferencedData#sizeOf() size} of data is below a given threshold. If the threshold is
 * exceeded or once evicted from the cache, the data is passed on to a delegate column store. The store allows
 * concurrent reading via multiple {@link ColumnDataReader ColumnDataReaders} once the {@link ColumnDataWriter} has been
 * closed.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class SmallColumnStore implements ColumnStore {

    private static final String ERROR_MESSAGE_ON_FLUSH = "Error while flushing small table.";

    private static final String ERROR_MESSAGE_ON_READ = "Error while reading small table.";

    /**
     * A cache for storing small tables that can be shared between multiple {@link SmallColumnStore SmallColumnStores}.
     */
    public static final class SmallColumnStoreCache {

        private final int m_smallTableThreshold;

        private final LoadingEvictingCache<SmallColumnStore, InMemoryColumnStore> m_cache;

        /**
         * @param smallTableThreshold the size (in bytes) that determines whether a table is considered small
         * @param cacheSize the number of small tables the cache should be able to hold
         */
        public SmallColumnStoreCache(final int smallTableThreshold, final long cacheSize) {
            m_smallTableThreshold = smallTableThreshold;
            m_cache = new SizeBoundLruCache<>(cacheSize);
        }

        int size() {
            return m_cache.size();
        }
    }

    private final class SmallColumnStoreWriter implements ColumnDataWriter {

        private InMemoryColumnStore m_table = new InMemoryColumnStore(getSchema());

        @SuppressWarnings("resource")
        @Override
        public void write(final ColumnData[] batch) throws IOException {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            m_numChunks++;

            if (m_table != null) {
                m_table.getWriter().write(batch);
                if (m_table.sizeOf() > m_smallTableThreshold) {
                    try {
                        m_table.getWriter().close();
                        flush(m_table);
                        m_table.close();
                    } catch (final Exception e) {
                        throw new IOException(ERROR_MESSAGE_ON_FLUSH, e);
                    } finally {
                        m_table = null;
                    }
                }
            } else {
                initDelegateWriterAndWrite(batch);
            }
        }

        @Override
        public void close() throws IOException {
            if (m_table != null) {
                m_table.getWriter().close();
                m_cache.retainAndPut(SmallColumnStore.this, m_table, (store, table) -> {
                    try {
                        flush(table);
                        closeDelegateWriter();
                        table.close(); // closing the table involves a call to release
                    } catch (Exception e) {
                        throw new IllegalStateException(ERROR_MESSAGE_ON_FLUSH, e);
                    }
                });
                m_table.release(); // from here on out, the cache is responsible for retaining
                m_table = null;
            } else {
                closeDelegateWriter();
            }
            m_writerClosed = true;
        }

    }

    private final class SmallColumnStoreReader implements ColumnDataReader {

        private final ColumnSelection m_selection;

        private final InMemoryColumnStore m_table = m_cache.retainAndGet(SmallColumnStore.this);

        // lazily initialized
        private ColumnDataReader m_delegateReader;

        private boolean m_readerClosed;

        SmallColumnStoreReader(final ColumnSelection selection) {
            m_selection = selection;
        }

        @Override
        public ColumnData[] read(final int chunkIndex) throws IOException {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            if (m_table != null) {
                try (final ColumnDataReader reader = m_table.createReader(m_selection)) {
                    return reader.read(chunkIndex);
                } catch (final Exception e) {
                    throw new IOException(ERROR_MESSAGE_ON_READ, e);
                }
            }

            if (m_delegateReader == null) {
                m_delegateReader = m_delegate.createReader(m_selection);
            }
            return m_delegateReader.read(chunkIndex);
        }

        @Override
        public void close() throws IOException {
            if (m_table != null) {
                m_table.release();
            }

            if (m_delegateReader != null) {
                m_delegateReader.close();
            }

            m_readerClosed = true;
        }

        @Override
        public int getNumChunks() {
            return m_numChunks;
        }

        @Override
        public int getMaxDataCapacity() {
            if (m_delegateReader == null) {
                m_delegateReader = m_delegate.createReader(m_selection);
            }
            return m_delegateReader.getMaxDataCapacity();
        }

    }

    private final ColumnStore m_delegate;

    private final ColumnDataFactory m_factory;

    private final int m_smallTableThreshold;

    private final LoadingEvictingCache<SmallColumnStore, InMemoryColumnStore> m_cache;

    private final SmallColumnStoreWriter m_writer;

    private boolean m_flushed = false;

    // lazily initialized
    private ColumnDataWriter m_delegateWriter;

    private int m_numChunks = 0;

    // this flag is volatile so that data written by the writer in some thread is visible to a reader in another thread
    private volatile boolean m_writerClosed;

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    /**
     * @param delegate the delegate to which to write if the table is not small
     * @param cache the cache for obtaining and storing small tables
     */
    public SmallColumnStore(final ColumnStore delegate, final SmallColumnStoreCache cache) {
        m_delegate = delegate;
        m_factory = delegate.getFactory();
        m_cache = cache.m_cache;
        m_smallTableThreshold = cache.m_smallTableThreshold;
        m_writer = new SmallColumnStoreWriter();
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public ColumnDataFactory getFactory() {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_factory;
    }

    @Override
    public SmallColumnStoreWriter getWriter() {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_writer;
    }

    private void initDelegateWriterAndWrite(final ColumnData[] batch) throws IOException {
        if (m_delegateWriter == null) {
            m_delegateWriter = m_delegate.getWriter();
        }
        m_delegateWriter.write(batch);
    }

    private void closeDelegateWriter() throws IOException {
        if (m_delegateWriter != null) {
            m_delegateWriter.close();
        }
    }

    private synchronized void flush(final InMemoryColumnStore table) throws IOException {
        if (!m_flushed) {
            try (final ColumnDataReader reader = table.createReader()) {
                for (int i = 0; i < reader.getNumChunks(); i++) {
                    final ColumnData[] batch = reader.read(i);
                    initDelegateWriterAndWrite(batch);
                    for (final ColumnData data : batch) {
                        data.release();
                    }
                }
            }
            m_flushed = true;
        }
    }

    @SuppressWarnings("resource")
    @Override
    public void saveToFile(final File file) throws IOException {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        final InMemoryColumnStore cached = m_cache.retainAndGet(SmallColumnStore.this);
        if (cached != null) {
            try {
                flush(cached);
                closeDelegateWriter();
            } catch (Exception e) {
                throw new IOException(ERROR_MESSAGE_ON_FLUSH, e);
            } finally {
                cached.release();
            }
        }

        m_delegate.saveToFile(file);
    }

    @Override
    public SmallColumnStoreReader createReader(final ColumnSelection selection) {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return new SmallColumnStoreReader(selection);
    }

    @Override
    public void close() throws IOException {
        try (final InMemoryColumnStore removed = m_cache.remove(SmallColumnStore.this)) {
            // closing the table involves a call to release
        }
        m_delegate.close();
        m_storeClosed = true;
    }

}
