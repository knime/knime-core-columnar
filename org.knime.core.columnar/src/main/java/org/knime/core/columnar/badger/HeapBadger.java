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
 *   20 Dec 2023 (pietzsch): created
 */
package org.knime.core.columnar.badger;

import static org.knime.core.columnar.badger.DebugLog.debug;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.knime.core.columnar.access.ColumnarAccessFactory;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory.ColumnarWriteCursor;
import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccessRow;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * The {@link HeapBadger} takes care of creating batches of roughly the same size, while offering a write cursor to the
 * outside. To know the size of a batch, we need to first serialize the data. This can potentially take a while, and
 * thus the serialization runs asynchronously in a separate thread. The write cursor fills a ring buffer of rows which
 * are then processed by the serialization thread.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @author Tobias Pietzsch
 * @since 5.3
 */
public class HeapBadger {

    // TODO we should make this depend on the size of the data that we know about in advance
    /** max number of rows in one batch */
    private static final int DEFAULT_MAX_NUM_ROWS_PER_BATCH = (1 << 15) - 750;

    /** max size of a batch */
    private static final int DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 1 << 26;

    /**
     * The executor service used if none is provided, used for benchmarks and tests. In regular AP execution the service
     * is set by core, respecting a node's <code>NodeContext</code>.
     */
    private static final ExecutorService FALLBACK_EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    private final BadgerWriteCursor m_writeCursor;

    private final Badger m_badger;

    private final BatchWritable m_writeDelegate;

    /**
     * Constructor
     *
     * @param writable
     * @param maxNumRowsPerBatch
     * @param maxBatchSizeInBytes
     * @param execService ... or null to use a fallback executor service
     */
    public HeapBadger(final BatchWritable writable, final int maxNumRowsPerBatch, final int maxBatchSizeInBytes,
        final ExecutorService execService) {
        ColumnarSchema schema = writable.getSchema();

        @SuppressWarnings("resource")
        int newBatchSize =
            Math.min(maxBatchSizeInBytes / writable.getWriter().initialNumBytesPerElement(), maxNumRowsPerBatch);

        int bufferSize = Math.min(20, newBatchSize);
        @SuppressWarnings("resource") // closed in Badger.close()
        final SerializationQueue async = new DefaultSerializationQueue(bufferSize);
        m_writeCursor = new BadgerWriteCursor(schema, async);
        m_badger = new Badger(writable, m_writeCursor.m_buffers, maxNumRowsPerBatch, maxBatchSizeInBytes);
        m_writeDelegate = writable;
        Objects.requireNonNullElse(execService, FALLBACK_EXECUTOR_SERVICE)
            .submit(() -> async.serializeLoop(m_badger));
    }

    /**
     * Constructor
     *
     * @param writable
     * @param execService
     */
    public HeapBadger(final BatchWritable writable, final ExecutorService execService) {
        this(writable, DEFAULT_MAX_NUM_ROWS_PER_BATCH, DEFAULT_MAX_BATCH_SIZE_IN_BYTES, execService);
    }

    /**
     * Constructor
     *
     * @param writable
     */
    public HeapBadger(final BatchWritable writable) {
        this(writable, null);
    }

    /**
     * @return the {@link ColumnarWriteCursor} that can be populated with data. Internally it fills a ring buffer that
     *         will then be serialized asynchronously.
     */
    public ColumnarWriteCursor getWriteCursor() {
        return m_writeCursor;
    }

    /**
     * @return The schema of the data
     */
    public ColumnarSchema getSchema() {
        return m_writeDelegate.getSchema();
    }

    /**
     * @return The number of batches written
     */
    public int numBatches() {
        return m_badger.getNumBatchesWritten();
    }

    // --------------------------------------------------------------------
    //
    //   Badger
    //

    static class Badger implements SerializationQueue.Serializer {

        private final BufferedAccessRow[] m_buffers;

        private final int m_bufferSize;

        private final int m_maxNumRowsPerBatch;

        private final int m_maxBatchSizeInBytes;

        private final BatchWriter m_writer;

        private WriteBatch m_current_batch;

        private final ColumnarWriteAccess[] m_accessesToTheCurrentBatch;

        private int m_batchLocalRowIndex;

        private int m_numBatchesWritten;

        Badger(final BatchWritable store, final BufferedAccessRow[] buffers, final int maxNumRowsPerBatch,
            final int maxBatchSizeInBytes) {
            m_buffers = buffers;
            m_bufferSize = buffers.length;

            m_writer = store.getWriter();
            m_current_batch = null;

            int initialNumBytesPerElement = m_writer.initialNumBytesPerElement();
            m_maxNumRowsPerBatch = Math.min(maxBatchSizeInBytes / initialNumBytesPerElement, maxNumRowsPerBatch);
            m_maxBatchSizeInBytes = maxBatchSizeInBytes;

            final ColumnarSchema schema = store.getSchema();
            final int numColumns = schema.numColumns();
            m_accessesToTheCurrentBatch = new ColumnarWriteAccess[numColumns];
            for (int c = 0; c < numColumns; ++c) {
                ColumnarAccessFactory factory = ColumnarAccessFactoryMapper.createAccessFactory(schema.getSpec(c));
                m_accessesToTheCurrentBatch[c] = factory.createWriteAccess(() -> m_batchLocalRowIndex);
            }
        }

        synchronized int getNumBatchesWritten() {
            // synchronized so that getNumBatchesWritten and writeCurrentBatch cannot be called at the same time
            return m_numBatchesWritten;
        }

        private void writeBufferedRow(final int row) throws IOException {
            debug("[b] Badger.writeBufferedRow( row={} )", row);
            final BufferedAccessRow bufferedRow = m_buffers[row];

            if (m_current_batch == null ) {
                switchToNextBatch();
            }

            // Set the data from the buffer
            for (int col = 0; col < m_accessesToTheCurrentBatch.length; ++col) {
                // Write to batch
                try {
                    m_accessesToTheCurrentBatch[col].setFrom(bufferedRow.getAccess(col));
                } catch (Exception e) { // NOSONAR: we really want to catch ALL exceptions that might be thrown by serializer
                    throw new IOException("Error during serialization: " + e.getMessage(), e);
                }
            }
            ++m_batchLocalRowIndex;

            debug("[b]       localRowIdx:        {}", m_batchLocalRowIndex);
            debug("[b]       maxNumRowsPerBatch: {}", m_maxNumRowsPerBatch);
            debug("[b]       sizeof batch:       {}", m_current_batch.usedSizeFor(m_batchLocalRowIndex));
            debug("[b]       max batch sizeof:   {}", m_maxBatchSizeInBytes);
            if (m_batchLocalRowIndex >= m_maxNumRowsPerBatch
                || m_current_batch.usedSizeFor(m_batchLocalRowIndex) >= m_maxBatchSizeInBytes) {
                debug("[b]   switch to new batch");
                // TODO if we have written more data in some columns make sure we do not loose it
                writeCurrentBatch();
            }
        }

        private synchronized void writeCurrentBatch() throws IOException {
            // synchronized so that getNumBatchesWritten and writeCurrentBatch cannot be called at the same time
            debug("[b] Badger.writeCurrentBatch");
            if ( m_current_batch != null ) {
                ReadBatch readBatch = m_current_batch.close(m_batchLocalRowIndex);
                m_current_batch = null;
                try {
                    m_writer.write(readBatch);
                } finally {
                    readBatch.release();
                }
                m_numBatchesWritten++;
            }
        }

        private void switchToNextBatch() {
            debug("[b] Badger.switchToNextBatch");

            // Create the next batch
            m_current_batch = m_writer.create(m_maxNumRowsPerBatch);

            // Connect the accesses with the current write batch
            for (int col = 0; col < m_accessesToTheCurrentBatch.length; col++) {
                m_accessesToTheCurrentBatch[col].setData(m_current_batch.get(col));
            }

            m_batchLocalRowIndex = 0;
        }

        /**
         * Write buffered rows to the underlying store. Split batches when they become large enough.
         */
        @Override
        public void serialize(final int from, final int to) throws IOException, InterruptedException {
            debug("[b] Badger.serialize( from={}, to={} )", from, to);
            for (int i = from; i < to; ++i) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("Serialization interrupted");
                }
                debug("[b]   writeBufferedRow({})  ... i={}", i % m_bufferSize, i);
                writeBufferedRow(i % m_bufferSize);
            }
        }

        @Override
        public void flush() throws IOException {
            debug("[b]  Badger.flush()");
            writeCurrentBatch();
            clearAccessesToCurrentBatch();
        }

        @Override
        public void close() throws IOException {
            debug("[b]  close Badger");
            if (m_current_batch != null) {
                m_current_batch.release();
                m_current_batch = null;
            }
            clearAccessesToCurrentBatch();
            m_writer.close();
        }

        private void clearAccessesToCurrentBatch() {
            for (ColumnarWriteAccess access : m_accessesToTheCurrentBatch) {
                access.setData(null);
            }
        }
    }

    // --------------------------------------------------------------------
    //
    //   BadgerWriteCursor
    //

    // TODO (TP): For now, I'm assuming that:
    //            WriteCursor should be treated as not thread-safe,
    //            except for flush() which may be called from the MemoryAlertSystem.
    //            That is: commit(), finish(), close() are called sequentially by the same thread.
    //            (or there is outside synchronization if a WriteCursor is handed off between threads).
    //            flush() however, might happen anytime.

    class BadgerWriteCursor implements ColumnarWriteCursor {
        private final BufferedAccessRow[] m_buffers;

        private final BufferedAccessRow m_access;

        private final SerializationQueue m_queue;

        private boolean m_closed;

        BadgerWriteCursor(final ColumnarSchema schema, final SerializationQueue queue) {
            m_queue = queue;
            m_buffers = new BufferedAccessRow[queue.getBufferSize()]; // TODO Async::getBufferSize() instead of field access
            Arrays.setAll(m_buffers, i -> BufferedAccesses.createBufferedAccessRow(schema));
            m_access = BufferedAccesses.createBufferedAccessRow(schema);
        }

        @Override
        public WriteAccessRow access() {
            return m_access;
        }

        private int m_current = 0;

        @Override
        public void commit() throws IOException {
            debug("[c:{}] BadgerWriteCursor.commit", this);
            if (m_closed) {
                throw new IllegalStateException("Cannot commit to a closed write cursor");
            }

            try {
                m_buffers[m_current].setFrom(m_access);
                debug("[c:{}]   -> m_queue.commit()", this);
                m_current = m_queue.commit();
            } catch (InterruptedException e) {
                // can throw InterruptedException if the cursor had to wait for free buffers to write to and got interrupted
                throw new RuntimeException(e); // let's pretend it is a RuntimeException?
            }
            debug("[c:{}]   <- m_queue.commit()", this);
            debug("[c:{}]   m_current = {}", this, m_current);
        }

        @Override
        public void flush() throws IOException {
            debug("[c:{}] BadgerWriteCursor.flush", this);
            if (m_closed) {
                // TODO (TP): should we rather
                //              throw new IllegalStateException("Cannot flush a closed write cursor");
                //            ???
                debug("[c:{}] !! already closed, ignoring call !!", this);
                return;
            }
            try {
                debug("[c:{}]   -> m_queue.flush()", this);
                m_queue.flush();
                debug("[c:{}]   <- m_queue.flush()", this);
            } catch (InterruptedException e) {
                throw new RuntimeException(e); // let's pretend it is a RuntimeException?
            }
        }

        @Override
        public void finish() throws IOException {
            debug("[c:{}] BadgerWriteCursor.finish", this);
            if (m_closed) {
                // TODO (TP): should we rather
                //            throw new IllegalStateException("Calling finish() on a WriteCursor that has already been closed.")?
                debug("[c:{}] !! already closed, ignoring call !!", this);
                return;
            }
            try {
                debug("[c:{}]   -> m_queue.finish()", this);
                m_queue.finish();
                m_closed = true;
                // NB: Setting m_closed here means that any subsequent close()
                // will not do anything. Therefore, m_queue.finish() should have
                // the same effects as m_queue.close(): exit the serialization
                // loop and close the queue.
                debug("[c:{}]   <- m_queue.finish()", this);
            } catch (InterruptedException e) {
                throw new RuntimeException(e); // let's pretend it is a RuntimeException?
            }
        }

        @Override
        public void close() throws IOException {
            // TODO abort, release resources, ignore exceptions that might have occurred while serializing in m_queue
            if (m_closed) {
                // TODO (TP): should we rather
                //            throw new IllegalStateException("Calling close() on a WriteCursor that has already been closed.")?
                debug("[c:{}] !! already closed, ignoring call !!", this);
                return;
            }

            m_closed = true;
            m_queue.close();

            // TODO (TP): clear m_buffers[]

            debug("[c:{}] --- Closing the Badger Write Cursor ", this);
        }

        @Override
        public long numRows() {
            return m_queue.numRows();
        }
    }
}
