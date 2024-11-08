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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.knime.core.columnar.access.ColumnarAccessFactory;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.badger.HeapCacheBuffers.HeapCacheBuffer;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.cache.object.shared.SoftReferencedObjectCache;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory.ColumnarWriteCursor;
import org.knime.core.columnar.store.BatchStore;
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

    private static final void debug(final String message) {
        //        System.out.println(message);
    }

    // TODO we should make this depend on the size of the data that we know about in advance
    /** max number of rows in one batch */
    private static final int DEFAULT_MAX_NUM_ROWS_PER_BATCH = (1 << 15) - 750;

    /** max size of a batch */
    private static final int DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 1 << 26;

    private final BadgerWriteCursor m_writeCursor;

    private final Badger m_badger;

    private final HeapCache m_heapCache;

    private final BatchWritable m_writeDelegate;

    private final Thread m_serializationThread;

    /**
     * Constructor
     *
     * @param writable
     * @param readable
     * @param maxNumRowsPerBatch
     * @param maxBatchSizeInBytes
     * @param cache
     */
    public HeapBadger(final BatchWritable writable, final RandomAccessBatchReadable readable,
        final int maxNumRowsPerBatch, final int maxBatchSizeInBytes, final SharedObjectCache cache) {
        ColumnarSchema schema = writable.getSchema();

        @SuppressWarnings("resource")
        int newBatchSize =
            Math.min(maxBatchSizeInBytes / writable.getWriter().initialNumBytesPerElement(), maxNumRowsPerBatch);

        int bufferSize = Math.min(20, newBatchSize);
        final SerializationQueue async = new AsyncQueue(bufferSize);
        //        final SerializationQueue async = new SyncQueue();
        m_writeCursor = new BadgerWriteCursor(schema, async);
        m_badger = new Badger(writable, m_writeCursor.m_buffers, maxNumRowsPerBatch, maxBatchSizeInBytes);
        m_heapCache = new HeapCache(readable, cache);
        m_writeDelegate = writable;

        m_serializationThread = new Thread(() -> {
            async.serializeLoop(m_badger);
        }, "HeapBadger-SerializationLoop");
        m_serializationThread.start();
    }

    /**
     * Constructor
     *
     * @param store
     */
    public HeapBadger(final BatchStore store) {
        this(store, store, DEFAULT_MAX_NUM_ROWS_PER_BATCH, DEFAULT_MAX_BATCH_SIZE_IN_BYTES,
            new SoftReferencedObjectCache());
    }

    /**
     * Constructor
     *
     * @param store
     * @param cache
     */
    public HeapBadger(final BatchStore store, final SharedObjectCache cache) {
        this(store, store, DEFAULT_MAX_NUM_ROWS_PER_BATCH, DEFAULT_MAX_BATCH_SIZE_IN_BYTES, cache);
    }

    /**
     * Constructor
     *
     * @param writable
     * @param readable
     * @param cache
     */
    public HeapBadger(final BatchWritable writable, final RandomAccessBatchReadable readable,
        final SharedObjectCache cache) {
        this(writable, readable, DEFAULT_MAX_NUM_ROWS_PER_BATCH, DEFAULT_MAX_BATCH_SIZE_IN_BYTES, cache);
    }

    HeapBadger(final BatchStore store, final int maxNumRowsPerBatch, final int maxBatchSizeInBytes) {
        this(store, store, maxNumRowsPerBatch, maxBatchSizeInBytes, new SoftReferencedObjectCache());
    }

    /**
     * @return the {@link ColumnarWriteCursor} that can be populated with data. Internally it fills a ring buffer that
     *         will then be serialized asynchronously.
     */
    public ColumnarWriteCursor getWriteCursor() {
        return m_writeCursor;
    }

    /**
     * @return The heap cache that has the "readable" interface and sees the data cached by this badger. Use the Badger
     *         in the write direction and the Cache in the read direction!
     */
    public HeapCache getHeapCache() {
        return m_heapCache;
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
    //   Async
    //
    interface SerializationQueue extends Closeable {
        interface Serializer {
            void serialize(int previous_head, int head) throws IOException, InterruptedException;

            void finish() throws IOException;

            void close() throws IOException;
        }

        /**
         * This worker method runs in a separate thread and serializes entries when they become available.
         *
         * @param serializer
         */
        void serializeLoop(Serializer serializer);

        /**
         * @return index of buffer to modify
         * @throws InterruptedException if interrupted while waiting
         * @throws IOException if a serializer has failed (in a separate thread) since the last call to forward
         */
        int forward() throws InterruptedException, IOException;

        /**
         * Blocks until all queued entries have been processed by the {@link Serializer}.
         * <p>
         * When {@code flush()} returns, {@link Serializer#serialize} has run on all entries and the queue is now empty.
         * It does not necessarily mean that everything is written to files, etc.
         *
         * @throws InterruptedException if interrupted while waiting
         * @throws IOException if a serializer has failed (in a separate thread) since the last call to forward
         */
        void flush() throws InterruptedException, IOException;

        /**
         * Waits for all queued serializations to finish.
         * <p>
         * When {@code finish()} returns {@link Serializer#serialize} has run on all entries, and @link
         * {@link Serializer#finish} has been called. No further entries are accepted after {@code finish()}.
         *
         * @throws InterruptedException if interrupted while waiting
         * @throws IOException if a serializer has failed (in a separate thread) since the last call to forward
         */
        void finish() throws InterruptedException, IOException;

        int getBufferSize();

        /**
         * Get the number of times {@link #forward()} has been called.
         *
         * @return number of calls to {@link #forward()}
         */
        long numForwards();
    }

    static class AsyncQueue implements SerializationQueue {
        private final int m_bufferSize;

        private final ReentrantLock m_lock;

        private final Condition m_notEmpty;

        private final Condition m_notFull;

        private final Condition m_finished;

        private final Condition m_closed;

        private Throwable m_exception = null; // only filled if an exception occurred, needs to be re-thrown at the next call of the serializer

        private boolean m_finishing;

        private boolean m_closing;

        private final int m_wrap_at;

        private int m_current;

        private int m_bound;

        private long m_offset;

        private SerializationQueue.Serializer m_serializer;

        AsyncQueue(final int bufferSize) {
            m_bufferSize = bufferSize;
            m_wrap_at = 2 * bufferSize;

            m_lock = new ReentrantLock();
            m_notEmpty = m_lock.newCondition();
            m_notFull = m_lock.newCondition();
            m_finished = m_lock.newCondition();
            m_closed = m_lock.newCondition();

            // initialize indices:

            m_current = 0;
            // After the most recent forward(), the cursor modifies buffers[m_current].
            // Initially, before the first forward(), m_current = 0, so the cursor modifies m_buffers[0].
            //
            // Via the Cursor contract, cursor.forward() needs to be called once, before modifying the first row.
            // So the Cursor running on top of Async needs to "swallow" the first cursor.forward().
            //
            // After forward(), everything up to m_buffers[m_current-1] is ready to be serialized.
            // So after the second forward(), m_current=1, and m_buffers[0] is valid. m_buffers[1] is now modified.
            //
            // When the serializer thread reads head:=m_current, it knows everything up to m_buffers[head-1] can be serialized.
            // The first index to be serialized is serializer.m_previous_head.
            // After the serializer is done serializing up to m_buffers[head-1], it sets m_previous_head:=head,
            //   because m_buffers[head] is the first to be serialized in the next round.

            m_bound = m_bufferSize;
            // forward() increments m_current, and blocks if m_current==m_bound afterwards.
            // Otherwise, the access() returned after forward() would write into the range that is currently serializing.
            //
            // If the first serialization has not finished (maybe it was not even triggered yet) when m_buffers is filled,
            //   m_current==m_bufferSize, that is access() would return the invalid m_buffers[m_bufferSize].
            // Technically, m_current should wrap around to 0 then, before the forward() returns.
            // However, from the perspective of the serializer, it would be indistinguishable whether nothing has been
            //   written since the last round, or everything.
            //
            // We disambiguate this by only wrapping to m_current=0 at 2*m_bufferSize,
            //   and taking indices modulo m_bufferSize for reading and writing.
            // That is:
            //   * access() modifies m_buffers[m_current % m_bufferSize].
            //   * serializer takes buffers from m_previous_head % m_bufferSize to (head-1) % m_bufferSize.

            m_offset = 0;
            // m_offset is used to calculate getNumForwards() as m_offset + m_current.
            // When wrapping at m_wrap_at, m_offset is incremented by m_wrap_at.
        }

        @Override
        public int getBufferSize() {
            return m_bufferSize;
        }

        @Override
        public int forward() throws InterruptedException, IOException {

            // TODO: throw Exception if finishing==true?
            //       @throws IllegalStateException if called after {@link #finish}

            // TODO: this needs to happen in WriteCursor: (swallow first forward())
            //    if (m_current >= 0) {
            //        m_buffers[m_current % m_bufferSize].setFrom(m_access);
            //    }

            rethrowExceptionInErrorCase();

            final ReentrantLock lock = this.m_lock;
            debug("[c] ACQUIRING LOCK IN FORWARD");
            lock.lock();
            debug("[c] LOCKED IN FORWARD");
            try {
                debug("[c]  Q m_current = " + m_current);
                debug("[c]  Q m_bound = " + m_bound);
                debug("[c]  Q m_offset = " + m_offset);
                if (++m_current == m_wrap_at) {
                    m_current = 0;
                    m_offset += m_wrap_at;
                }
                while (m_current == m_bound && m_exception == null) {
                    debug("[c]  Q -> m_notFull.await();");
                    m_notFull.await();
                    debug("[c]  Q <- m_notFull.await();");
                }
                m_notEmpty.signal();
                debug("[c]  Q m_current = " + m_current);
                debug("[c]  Q m_bound = " + m_bound);
                debug("[c]  Q m_offset = " + m_offset);
            } finally {
                debug("[c] UNLOCKING IN FORWARD");
                lock.unlock();
            }

            rethrowExceptionInErrorCase();

            return m_current % m_bufferSize;
        }

        @Override
        public long numForwards() {
            final ReentrantLock lock = this.m_lock;
            lock.lock();
            try {
                if (!m_finishing) {
                    throw new IllegalStateException("Accessed size of table before closing cursor");
                }
                return m_offset + m_current;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void flush() throws InterruptedException, IOException {
            final ReentrantLock lock = this.m_lock;
            lock.lock();
            try {
                while (!isQueueEmpty() && m_exception == null) {
                    m_notFull.await();
                }
            } finally {
                lock.unlock();
            }

            rethrowExceptionInErrorCase();
        }

        private boolean isQueueEmpty() {
            return m_bound == (m_current + m_bufferSize) % m_wrap_at;
        }

        @Override
        public void finish() throws InterruptedException, IOException {
            final ReentrantLock lock = this.m_lock;
            lock.lock();
            try {
                if (m_finishing) {
                    return; // TODO: actually wait for the other finishing calls to finish, too
                    // TODO (TP): Is this still relevant? What are "the other finishing calls"?
                    //            In general: Should SerializationQueue do any IllegalState checking,
                    //            or should this rather all be done at the WriteCursor level?
                }
                m_finishing = true;
                m_notEmpty.signal();
                m_finished.await(); // is reached definitely if errors occurred
            } finally {
                lock.unlock();
            }

            rethrowExceptionInErrorCase();
        }

        private void rethrowExceptionInErrorCase() throws InterruptedException, IOException {
            if (m_exception != null) {
                if (m_exception instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                } else if (m_exception instanceof InterruptedException interruptedException) {
                    throw interruptedException;
                } else if (m_exception instanceof IOException ioException) {
                    throw ioException;
                }
                throw new IOException(m_exception);
            }
        }

        @Override
        public void serializeLoop(final Serializer serializer) {
            m_serializer = serializer;
            final ReentrantLock lock = this.m_lock;
            int previous_head = 0;
            int head;
            boolean doClose;
            boolean doFinish;
            while (true) {
                debug("[b] - 0 -");
                debug("[b] - previous_head = " + previous_head);
                debug("[b] ACQUIRING LOCK IN LOOP");
                lock.lock();
                debug("[b] LOCKED IN LOOP");
                try {
                    head = m_current;
                    doClose = m_closing;
                    doFinish = m_finishing;
                    debug("[b] - 1 -");
                    debug("[b] - head = " + head);
                    debug("[b] - doFinish = " + doFinish);
                    while (head == previous_head && !doFinish && !doClose) {
                        debug("[b] - -> m_notEmpty.await();");
                        try {
                            m_notEmpty.await();
                        } catch (InterruptedException ex) {
                            // in case of an exception, we remember it and quit the serialization loop
                            m_exception = ex;
                            // to unblock forward() we need to claim that there's more space, but it'll rethrow the exception
                            m_notFull.signal();
                            m_closed.signal();
                            m_finished.signal();
                            debug("[b] ERROR EXIT a) because of " + ex.getMessage());
                            return;
                        }
                        debug("[b] - <- m_notEmpty.await();");
                        head = m_current;
                        doClose = m_closing;
                        doFinish = m_finishing;
                        debug("[b] - head = " + head);
                        debug("[b] - doFinish = " + doFinish);
                    }
                } finally {
                    debug("[b] UNLOCKING IN LOOP");
                    lock.unlock();
                }

                debug("[b] - 2 -");
                // Note that m_previous_head..head maybe empty in case we are finishing
                try {
                    serializer.serialize(previous_head, head);
                    if (doFinish) {
                        serializer.finish();
                    }
                } catch (Exception e) {
                    // in case of an exception, we remember it and quit the serialization loop
                    m_exception = e;
                    debug("[b] LOOP ERROR EXIT -- acquiring LOCK");
                    lock.lock();
                    debug("[b] LOOP ERROR EXIT -- LOCKING");
                    // to unblock forward() we need to claim that there's more space, but it'll rethrow the exception
                    m_notFull.signal();
                    m_closed.signal();
                    m_finished.signal();
                    debug("[b] LOOP ERROR EXIT -- UNLOCKING");
                    lock.unlock();
                    debug("[b] ERROR EXIT b) because of " + e.getMessage());
                    return;
                }

                debug("[b] - 3 -");
                debug("[b] ACQUIRING LOCK AT END OF ITERATION");
                lock.lock();
                debug("[b] LOCKED AT END OF ITERATION");
                try {
                    m_bound = (head + m_bufferSize) % m_wrap_at;
                    m_notFull.signal();
                    if (doFinish) {
                        m_finished.signal();
                        return;
                    }
                    if (doClose) {
                        m_closed.signal();
                        return;
                    }
                } finally {
                    debug("[b] UNLOCKING AT END OF ITERATION");
                    lock.unlock();
                }
                debug("[b] - 4 -");

                previous_head = head;
            }
        }

        @Override
        public void close() throws IOException {
            final ReentrantLock lock = this.m_lock;
            debug("[b] Close -- acquiring LOCK");
            lock.lock();
            debug("[b] Close -- LOCKING");
            try {
                if (m_exception != null) {
                    return;
                }

                if (m_closing) {
                    return; // multiple threads could call close
                }

                if (!m_finishing) {
                    debug("[b] Close -- waiting for closed signal");
                    m_closing = true;
                    m_closed.await();
                }

                debug("[b]  close AsyncQueue");
            } catch (InterruptedException ex) {
                throw new IOException(ex);
            } finally {
                m_serializer.close();
                lock.unlock();
                debug("[b] Close -- UNLOCKING");
            }
        }
    }

    /**
     * Synchronous serialization queue implementation that is supposed to be useful for debugging. NOTE: not tested very
     * well yet, might have bugs.
     */
    static class SyncQueue implements SerializationQueue {
        private int m_current = -1;

        private Serializer m_serializer;

        private boolean m_finished;

        @Override
        public synchronized int forward() throws InterruptedException, IOException {
            if (m_serializer != null) {
                m_serializer.serialize(0, 1);
                m_current++;
            }
            return 0;
        }

        @Override
        public synchronized long numForwards() {
            if (!m_finished) {
                throw new IllegalStateException("queried the size of the table before closing the cursor");
            }
            return m_current;
        }

        @Override
        public void flush() throws InterruptedException, IOException {
            // NO OP
        }

        @Override
        public synchronized void finish() throws InterruptedException, IOException {
            // NO OP
            m_finished = true;
        }

        @Override
        public synchronized void serializeLoop(final SerializationQueue.Serializer serializer) {
            m_serializer = serializer;
        }

        @Override
        public int getBufferSize() {
            return 1;
        }

        @Override
        public void close() throws IOException {
            // NO OP
        }
    }

    // --------------------------------------------------------------------
    //
    //   Badger
    //

    class Badger implements SerializationQueue.Serializer {

        private final BufferedAccessRow[] m_buffers;

        private final int m_bufferSize;

        private final int m_maxNumRowsPerBatch;

        private final int m_maxBatchSizeInBytes;

        private final BatchWriter m_writer;

        private WriteBatch m_current_batch;

        private final ColumnarWriteAccess[] m_accessesToTheCurrentBatch;

        private int m_batchLocalRowIndex;

        private final HeapCacheBuffer<?>[] m_heapCacheBuffers;

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
            m_heapCacheBuffers = HeapCacheBuffers.createHeapCacheBuffers(schema);
            for (int c = 0; c < numColumns; ++c) {
                ColumnarAccessFactory factory = ColumnarAccessFactoryMapper.createAccessFactory(schema.getSpec(c));
                m_accessesToTheCurrentBatch[c] = factory.createWriteAccess(() -> m_batchLocalRowIndex);
            }

            switchToNextBatch();
        }

        synchronized int getNumBatchesWritten() {
            // synchronized so that getNumBatchesWritten and writeCurrentBatch cannot be called at the same time
            return m_numBatchesWritten;
        }

        private void writeBufferedRow(final int row) throws IOException {
            debug("[b] Badger.writeBufferedRow( row=" + row + " )");
            final BufferedAccessRow bufferedRow = m_buffers[row];

            // Set the data from the buffer
            for (int col = 0; col < m_accessesToTheCurrentBatch.length; ++col) {
                var access = bufferedRow.getAccess(col);

                // Put all string and var binary data into a heap list/array to be added to the cache later.
                // Does not serialize objects but keeps a reference.

                m_heapCacheBuffers[col].getAccess(m_batchLocalRowIndex).setFrom(access);

                // Write to batch
                try {
                    m_accessesToTheCurrentBatch[col].setFrom(access);
                } catch (Exception e) { // NOSONAR: we really want to catch ALL extensions that might be thrown by serializer
                    throw new IOException("Error during serialization: " + e.getMessage(), e);
                }
            }
            ++m_batchLocalRowIndex;

            debug("[b]       localRowIdx:        " + m_batchLocalRowIndex);
            debug("[b]       maxNumRowsPerBatch: " + m_maxNumRowsPerBatch);
            debug("[b]       sizeof batch:       " + m_current_batch.usedSizeFor(m_batchLocalRowIndex));
            debug("[b]       max batch sizeof:   " + m_maxBatchSizeInBytes);
            if (m_batchLocalRowIndex >= m_maxNumRowsPerBatch
                || m_current_batch.usedSizeFor(m_batchLocalRowIndex) >= m_maxBatchSizeInBytes) {
                debug("[b]   switch to new batch");
                // TODO if we have written more data in some columns make sure we do not loose it
                writeCurrentBatch();
                switchToNextBatch();
            }
        }

        private synchronized void writeCurrentBatch() throws IOException {
            // synchronized so that getNumBatchesWritten and writeCurrentBatch cannot be called at the same time
            debug("[b] Badger.writeCurrentBatch");
            ReadBatch readBatch = m_current_batch.close(m_batchLocalRowIndex);
            m_writer.write(readBatch);

            // save data in cache
            for (int col = 0; col < m_writeDelegate.getSchema().numColumns(); col++) {
                var data = m_heapCacheBuffers[col].getArray();
                if (null != data) {
                    m_heapCache.cacheData((Object[])data,
                        new ColumnDataUniqueId(m_heapCache, col, m_numBatchesWritten));
                }
            }
            readBatch.release();
            m_current_batch = null;
            m_numBatchesWritten++;
        }

        private void switchToNextBatch() {
            debug("[b] Badger.switchToNextBatch");

            // Create the next batch
            m_current_batch = m_writer.create(m_maxNumRowsPerBatch);

            // Connect the accesses with the current write batch
            for (int col = 0; col < m_accessesToTheCurrentBatch.length; col++) {
                m_accessesToTheCurrentBatch[col].setData(m_current_batch.get(col));
                m_heapCacheBuffers[col].init(m_maxNumRowsPerBatch);
            }

            m_batchLocalRowIndex = 0;
        }

        /**
         * Write buffered rows to the underlying store. Split batches when they become large enough.
         */
        @Override
        public void serialize(final int previous_head, final int head) throws IOException, InterruptedException {
            debug("[b] Badger.serialize( previous_head=" + previous_head + ", head=" + head + " )");
            int from = previous_head;
            int to = head;
            if (to < from) {
                from -= m_bufferSize;
                to += m_bufferSize;
            }
            for (int i = from; i < to; ++i) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("Serialization interrupted");
                }
                debug("[b]   writeBufferedRow(" + (i % m_bufferSize) + ")  ... i=" + i);
                writeBufferedRow(i % m_bufferSize);
            }
        }

        @Override
        public void finish() throws IOException {
            writeCurrentBatch();
            m_writer.close();
        }

        @Override
        public void close() throws IOException {
            debug("[b]  close Badger");
            if (m_current_batch != null) {
                m_current_batch.release();
            }
            m_writer.close();
        }
    }

    // --------------------------------------------------------------------
    //
    //   BadgerWriteCursor
    //

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

        private int m_current = -1;

        @Override
        public boolean forward() {
            debug("[c:" + this + "] BadgerWriteCursor.forward");
            if (m_closed) {
                throw new IllegalStateException("Cannot forward a closed write cursor");
            }

            if (m_current < 0) {
                m_current = 0;
                return true;
            }

            try {
                m_buffers[m_current].setFrom(m_access);
                debug("[c:" + this + "]   -> m_queue.forward()");
                m_current = m_queue.forward();
            } catch (IOException | InterruptedException ex) {
                // can throw IOException if serialization of any previous row has failed
                // can throw InterruptedException if the cursor had to wait for free buffers to write to and got interrupted
                throw new RuntimeException(ex);
            }
            debug("[c:" + this + "]   <- m_queue.forward()");
            debug("[c:" + this + "]   m_current = " + m_current);
            return true;
        }

        @Override
        public void flush() throws IOException {
            debug("[c:" + this + "] BadgerWriteCursor.flush");
            if (m_closed) {
                // TODO (TP): should we rather
                //              throw new IllegalStateException("Cannot flush a closed write cursor");
                //            ???
                debug("[c:" + this + "] !! already closed, ignoring call !!");
                return;
            }
            try {
                debug("[c:" + this + "]   -> m_queue.flush()");
                m_queue.flush();
                debug("[c:" + this + "]   <- m_queue.flush()");
            } catch (InterruptedException e) {
                throw new RuntimeException(e); // let's pretend it is a RuntimeException?
            }
        }

        @Override
        public void finish() throws IOException {
            debug("[c:" + this + "] BadgerWriteCursor.finish");
            if (m_closed) {
                // TODO (TP): should we rather
                //              throw new IllegalStateException("Cannot finish a closed write cursor");
                //            ???
                debug("[c:" + this + "] !! already closed, ignoring call !!");
                return;
            }
            try {
                debug("[c:" + this + "]   -> m_queue.finish()");
                m_queue.finish();
                m_closed = true;
                debug("[c:" + this + "]   <- m_queue.finish()");
            } catch (InterruptedException e) {
                throw new RuntimeException(e); // let's pretend it is a RuntimeException?
            }
        }

        @Override
        public void close() throws IOException {
            // TODO abort, release resources, ignore exceptions that might have occurred while serializing in m_queue
            if (m_closed) {
                return;
            }

            m_closed = true;
            m_serializationThread.interrupt();
            m_queue.close();
            debug("[c:" + this + "] --- Closing the Badger Write Cursor ");
        }

        @Override
        public long getNumForwards() {
            return (m_current < 0) ? 0 : (m_queue.numForwards());
        }
    }
}
