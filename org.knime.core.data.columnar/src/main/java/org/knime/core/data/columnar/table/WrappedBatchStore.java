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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.data.columnar.table.WrappedBatchReadStore.WrappedRandomAccessBatchReader;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link BatchStore} that delegates operations to a {@link BatchWritable} and a {@link RandomAccessBatchReadable}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 *
 * @noreference This class is not intended to be referenced by clients
 */
public final class WrappedBatchStore implements BatchStore {

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

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

    private final BatchWritable m_writable;

    private final RandomAccessBatchReadable m_readable;

    private final AtomicBoolean m_storeClosed = new AtomicBoolean();

    private final AtomicBoolean m_writerClosed = new AtomicBoolean();

    private final WrappedBatchWriter m_writer;

    /**
     * Create a wrapped batch store from a {@link BatchWritable} and a {@link RandomAccessBatchReadable}.
     * @param writable The {@link BatchWritable}
     * @param readable The {@link RandomAccessBatchReadable}
     */
    public WrappedBatchStore(final BatchWritable writable, final RandomAccessBatchReadable readable) {
        m_writable = writable;
        m_readable = readable;
        m_writer = new WrappedBatchWriter(m_writable, m_storeClosed, m_writerClosed);
    }

    @Override
    public final ColumnarSchema getSchema() {
        return m_readable.getSchema();
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
    public final RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        if (!m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return new WrappedRandomAccessBatchReader(m_readable, selection, m_storeClosed, numBatches());
    }

    @Override
    public final void close() throws IOException {
        if (!m_storeClosed.getAndSet(true)) {
            m_writer.close();
            m_readable.close();
        }
    }

}
