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
 *   Oct 13, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.parallel.write;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.DataTransfers;
import org.knime.core.columnar.data.DataTransfers.DataTransfer;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.parallel.exec.DataWriter;
import org.knime.core.columnar.store.BatchStore;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class AsyncBatchWriter implements Closeable {

    private final BatchWriter m_writer;

    private WriteBatch m_currentBatch;

    private final AtomicInteger m_finalizeBatchBarrier;

    private final int m_batchLength;

    private final AsyncDataWriter[] m_columnWriters;

    public AsyncBatchWriter(final BatchStore batchStore, final int batchLength) {
        m_batchLength = batchLength;
        m_writer = batchStore.getWriter();
        final var schema = batchStore.getSchema();
        m_finalizeBatchBarrier = new AtomicInteger(schema.numColumns());
        m_columnWriters = schema.specStream()//
            .map(DataTransfers::createTransfer)//
            .map(AsyncDataWriter::new)//
            .toArray(AsyncDataWriter[]::new);
        startBatch();
    }

    public DataWriter getDataWriter(final int colIdx) {
        return m_columnWriters[colIdx];
    }

    private void switchBatch() {
        finishBatch(m_batchLength);
        startBatch();
    }

    private void startBatch() {
        m_currentBatch = m_writer.create(m_batchLength);
        resetColumnWriters();
    }

    private void finishBatch(final int batchLength) {
        assert allColumnWritersHaveExpectedIndex(batchLength) : "The column writers are out of sync.";
        var finishedBatch = m_currentBatch.close(batchLength);
        try {
            m_writer.write(finishedBatch);
        } catch (IOException ex) {
            // TODO we probably need a more elaborate mechanism to properly work in the concurrent setting
            throw new IllegalStateException("Failed to write batch.", ex);
        }
        finishedBatch.release();
    }

    public void finishLastBatch() throws IOException {
        var currentBatchLength = getCurrentBatchLength();
        if (currentBatchLength > 0) {
            finishBatch(currentBatchLength);
        }
        close();
    }

    private int getCurrentBatchLength() {
        return m_columnWriters[0].currentIdx();
    }

    private boolean allColumnWritersHaveExpectedIndex(final int expected) {
        return Stream.of(m_columnWriters).mapToInt(AsyncDataWriter::currentIdx).allMatch(i -> i == expected);
    }

    private void resetColumnWriters() {
        int numColumns = m_currentBatch.numData();
        m_finalizeBatchBarrier.set(numColumns);
        for (int i = 0; i < numColumns; i++) {
            m_columnWriters[i].reset(m_currentBatch.get(i));
        }

    }

    @Override
    public void close() throws IOException {
        // TODO how to tell the column writers that we are done?
        m_currentBatch = null;
        m_writer.close();
    }

    private final class AsyncDataWriter implements DataWriter {

        private final DataTransfer m_transfer;

        private NullableWriteData m_writeData;

        private final AtomicInteger m_idx = new AtomicInteger(0);

        int currentIdx() {
            return m_idx.get();
        }

        AsyncDataWriter(final DataTransfer transfer) {
            m_transfer = transfer;
        }

        void reset(final NullableWriteData writeData) {
            m_writeData = writeData;
            m_idx.set(0);
        }

        @Override
        public boolean accept(final NullableReadData readData, final int idx) {
            if (m_idx.get() == m_batchLength) {
                return false;
            }
            m_transfer.transfer(readData, idx, m_writeData, m_idx.get());
            if (m_idx.incrementAndGet() == m_batchLength && m_finalizeBatchBarrier.decrementAndGet() == 0) {
                // this is the last column to finish writing in this batch
                switchBatch();
            }
            return true;

        }
    }

}