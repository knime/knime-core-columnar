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
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link BatchReadStore} that delegates operations to a {@link RandomAccessBatchReadable}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class WrappedBatchReadStore implements BatchReadStore {

    static final class WrappedRandomAccessBatchReader implements RandomAccessBatchReader {

        private final RandomAccessBatchReadable m_delegateReadable;

        private final ColumnSelection m_selection;

        private final AtomicBoolean m_storeClosed;

        private final int m_numBatches;

        private final AtomicBoolean m_readerClosed;

        private RandomAccessBatchReader m_delegateReader;

        /**
         * @param store a delegating store from which to obtain the delegate reader
         * @param selection see {@link BatchReadStore#createRandomAccessReader(ColumnSelection)}
         */
        WrappedRandomAccessBatchReader(final RandomAccessBatchReadable wrappedDelegateReadable,
            final ColumnSelection selection, final AtomicBoolean storeClosed, final int numBatches) {
            m_delegateReadable = wrappedDelegateReadable;
            m_selection = selection;
            m_storeClosed = storeClosed;
            m_numBatches = numBatches;
            m_readerClosed = new AtomicBoolean();
        }

        @SuppressWarnings("resource")
        @Override
        public final ReadBatch readRetained(final int index) throws IOException {
            if (m_readerClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }
            if (index < 0) {
                throw new IndexOutOfBoundsException(String.format("Batch index %d smaller than 0.", index));
            }
            if (index >= m_numBatches) {
                throw new IndexOutOfBoundsException(
                    String.format("Batch index %d greater or equal to the reader's largest batch index (%d).", index,
                        m_numBatches - 1));
            }

            return initAndGetDelegate().readRetained(index);
        }

        @Override
        public final void close() throws IOException {
            if (!m_readerClosed.getAndSet(true) && m_delegateReader != null) {
                m_delegateReader.close();
            }
        }

        private final RandomAccessBatchReader initAndGetDelegate() {
            if (m_delegateReader == null) {
                m_delegateReader = m_delegateReadable.createRandomAccessReader(m_selection);
            }
            return m_delegateReader;
        }

    }

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column read store has already been closed.";

    private static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    private final RandomAccessBatchReadable m_readable;

    private final int m_numBatches;

    private final int m_batchLength;

    private final Path m_path;

    private final AtomicBoolean m_closed = new AtomicBoolean();

    WrappedBatchReadStore(final RandomAccessBatchReadable readable, final int numBatches, final int batchLength,
        final Path path) {
        m_readable = readable;
        m_numBatches = numBatches;
        m_batchLength = batchLength;
        m_path = path;
    }

    @Override
    public final RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        if (m_closed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return new WrappedRandomAccessBatchReader(m_readable, selection, m_closed, numBatches());
    }

    @Override
    public final ColumnarSchema getSchema() {
        return m_readable.getSchema();
    }

    @Override
    public int numBatches() {
        return m_numBatches;
    }

    @Override
    public int batchLength() {
        return m_batchLength;
    }

    @Override
    public Path getPath() {
        return m_path;
    }

    @Override
    public final void close() throws IOException {
        if (!m_closed.getAndSet(true)) {
            m_readable.close();
        }
    }

}
