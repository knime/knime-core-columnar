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
import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnStore} that holds {@link ColumnData} retained in memory until closed. The store allows concurrent
 * reading via multiple {@link ColumnDataReader ColumnDataReaders} once the {@link ColumnDataWriter} has been closed.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class InMemoryColumnStore implements ColumnStore, ReferencedData {

    private final class InMemoryColumnStoreFactory implements ColumnDataFactory {

        @Override
        public ColumnData[] create() {
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            throw new UnsupportedOperationException("Creating new ColumnData not supported by in-memory column store.");
        }

    }

    private final class InMemoryColumnStoreWriter implements ColumnDataWriter {

        @Override
        public void write(final ColumnData[] batch) throws IOException {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            for (final ColumnData data : batch) {
                data.retain();
                m_sizeOf += data.sizeOf();
                m_maxDataCapacity = Math.max(data.getMaxCapacity(), m_maxDataCapacity); //TODO revisit later
            }
            m_batches.add(batch);
        }

        @Override
        public void close() {
            m_writerClosed = true;
        }

    }

    private final class InMemoryColumnStoreReader implements ColumnDataReader {

        private final ColumnSelection m_selection;

        private boolean m_readerClosed;

        InMemoryColumnStoreReader(final ColumnSelection selection) {
            m_selection = selection;
        }

        @Override
        public ColumnData[] read(final int index) throws IOException {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            final ColumnData[] batch = m_batches.get(index);

            if (m_selection != null && m_selection.get() != null) {
                final int[] indices = m_selection.get();
                final ColumnData[] projected = new ColumnData[indices.length];
                for (int i = 0; i < indices.length; i++) {
                    projected[i] = batch[indices[i]];
                    projected[i].retain();
                }
                return projected;
            }

            for (final ColumnData data : batch) {
                data.retain();
            }
            return batch;
        }

        @Override
        public int getNumChunks() {
            return m_batches.size();
        }

        @Override
        public int getMaxDataCapacity() {
            return m_maxDataCapacity;
        }

        @Override
        public void close() {
            m_readerClosed = true;
        }

    }

    private final ColumnStoreSchema m_schema;

    private final List<ColumnData[]> m_batches = new ArrayList<>();

    private final ColumnDataFactory m_factory;

    private final ColumnDataWriter m_writer;

    private int m_sizeOf = 0;

    private int m_maxDataCapacity = 0;

    // this flag is volatile so that data written by the writer in some thread is visible to a reader in another thread
    private volatile boolean m_writerClosed;

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    /**
     * @param schema the schema of the table to be stored
     */
    public InMemoryColumnStore(final ColumnStoreSchema schema) {
        m_schema = schema;
        m_factory = new InMemoryColumnStoreFactory();
        m_writer = new InMemoryColumnStoreWriter();
    }

    @Override
    public void release() {
        for (final ColumnData[] batch : m_batches) {
            for (final ColumnData data : batch) {
                data.release();
            }
        }
    }

    @Override
    public void retain() {
        for (final ColumnData[] batch : m_batches) {
            for (final ColumnData data : batch) {
                data.retain();
            }
        }
    }

    @Override
    public int sizeOf() {
        return m_sizeOf;
    }

    @Override
    public ColumnDataFactory getFactory() {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_factory;
    }

    @Override
    public ColumnDataWriter getWriter() {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_writer;
    }

    @Override
    public void saveToFile(final File f) throws IOException {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        throw new UnsupportedOperationException("Saving to file not supported by in-memory column store.");
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection selection) {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return new InMemoryColumnStoreReader(selection);
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_schema;
    }

    @Override
    public void close() {
        if (!m_storeClosed) {
            release();
        }
        m_storeClosed = true;
    }
}
