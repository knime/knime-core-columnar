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
 *   21 Mar 2024 (chaubold): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * The BatchSizeRecorder can wrap a {@link BatchWritable}, record which sizes of batches are written, and then augment a
 * {@link RandomAccessBatchReadable} so that it knows the (written) batch sizes without having to read them from disk.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
class BatchSizeRecorder {
    private List<Long> m_batchBoundaries = new ArrayList<>();

    void recordBatch(final long numRowsInBatch) {
        m_batchBoundaries.add(numRowsInBatch + numRows());
    }

    BatchWritable wrap(final BatchWritable writable) {
        // TODO: this only makes sense if it is only called once
        return new BatchSizeRecordingWritable(writable);
    }

    RandomAccessBatchReadable augment(final RandomAccessBatchReadable readable) {
        return new BatchSizeAugmentedReadable(readable);
    }

    private long numRows() {
        return m_batchBoundaries.isEmpty() ? 0 : m_batchBoundaries.get(m_batchBoundaries.size() - 1);
    }

    /** package private so it is visible in tests */
    class BatchSizeRecordingWritable implements BatchWritable {
        private final BatchWritable m_delegate;

        private final BatchWriter m_writer;

        @SuppressWarnings("resource") // writer is closed with wrapper
        private BatchSizeRecordingWritable(final BatchWritable delegate) {
            m_delegate = delegate;
            m_writer = new BatchSizeRecordingWriter(m_delegate.getWriter());
        }

        @Override
        public BatchWriter getWriter() {
            return m_writer;
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_delegate.getSchema();
        }

        /** for tests */
        BatchWritable getDelegate() {
            return m_delegate;
        }
    }

    private class BatchSizeRecordingWriter implements BatchWriter {
        private final BatchWriter m_delegateWriter;

        private BatchSizeRecordingWriter(final BatchWriter writer) {
            m_delegateWriter = writer;
        }

        @Override
        public void close() throws IOException {
            m_delegateWriter.close();
        }

        @Override
        public WriteBatch create(final int capacity) {
            return m_delegateWriter.create(capacity);
        }

        @Override
        public void write(final ReadBatch batch) throws IOException {
            recordBatch(batch.length());
            m_delegateWriter.write(batch);
        }

        @Override
        public int initialNumBytesPerElement() {
            return m_delegateWriter.initialNumBytesPerElement();
        }

    }

    private class BatchSizeAugmentedReadable implements RandomAccessBatchReadable {
        private final RandomAccessBatchReadable m_delegate;

        private BatchSizeAugmentedReadable(final RandomAccessBatchReadable delegate) {
            m_delegate = delegate;
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_delegate.getSchema();
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
        }

        @Override
        public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
            return m_delegate.createRandomAccessReader(selection);
        }

        @Override
        public long[] getBatchBoundaries() {
            return m_batchBoundaries.stream().mapToLong(l -> l).toArray();
        }
    }
}
