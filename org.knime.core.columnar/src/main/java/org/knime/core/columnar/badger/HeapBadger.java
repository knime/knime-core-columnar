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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.knime.core.columnar.access.ColumnarAccessFactory;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.badger.HeapCacheBuffers.HeapCacheBuffer;
import org.knime.core.columnar.batch.BatchWriter;
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
 *
 * @author pietzsch
 */
public class HeapBadger {

    // TODO we should make this depend on the size of the data that we know about in advance
    /** max number of rows in one batch */
    private static final int DEFAULT_MAX_NUM_ROWS_PER_BATCH = 500;

    /** max size of a batch */
    private static final int DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 5000;

    private final BadgerWriteCursor m_writeCursor;

    private final Badger m_badger;

    private final HeapCache m_heapCache;

    private final BatchStore m_store;

    public HeapBadger(final BatchStore store, final int maxNumRowsPerBatch, final int maxBatchSizeInBytes,
        final SharedObjectCache cache) {
        ColumnarSchema schema = store.getSchema();

        int bufferSize = 20;
        final Async async = new Async(bufferSize);
        m_writeCursor = new BadgerWriteCursor(schema, async);
        m_badger = new Badger(store, m_writeCursor.m_buffers, maxNumRowsPerBatch, maxBatchSizeInBytes);
        m_heapCache = new HeapCache(store, cache);
        m_store = store;

        new Thread(() -> {
            try {
                async.serializeLoop(m_badger);
            } catch (InterruptedException e) {
                throw new RuntimeException(e); // TODO exception handling
            }
        }).start();
    }

    public HeapBadger(final BatchStore store) {
        this(store, DEFAULT_MAX_NUM_ROWS_PER_BATCH, DEFAULT_MAX_BATCH_SIZE_IN_BYTES, new SoftReferencedObjectCache());
    }

    HeapBadger(final BatchStore store, final SharedObjectCache cache) {
        this(store, DEFAULT_MAX_NUM_ROWS_PER_BATCH, DEFAULT_MAX_BATCH_SIZE_IN_BYTES, cache);
    }

    HeapBadger(final BatchStore store, final int maxNumRowsPerBatch, final int maxBatchSizeInBytes) {
        this(store, maxNumRowsPerBatch, maxBatchSizeInBytes, new SoftReferencedObjectCache());
    }

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

    // --------------------------------------------------------------------
    //
    //   Async
    //
    static class Async {
        private final int m_bufferSize;

        private final ReentrantLock m_lock;
        private final Condition m_notEmpty;
        private final Condition m_notFull;
        private final Condition m_finished;
        private boolean m_finishing;


        private final int m_wrap_at;
        private int m_current;
        private int m_bound;

        private long m_offset;

        Async(final int bufferSize) {
            m_bufferSize = bufferSize;
            m_wrap_at = 2 * bufferSize;

            m_lock = new ReentrantLock();
            m_notEmpty = m_lock.newCondition();
            m_notFull = m_lock.newCondition();
            m_finished = m_lock.newCondition();


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

        /**
         * @return index of buffer to modify
         * @throws InterruptedException if interrupted while waiting
         */
        int forward() throws InterruptedException {

            // TODO: throw Exception if finishing==true?
            //       @throws IllegalStateException if called after {@link #finish}

            // TODO: this needs to happen in WriteCursor: (swallow first forward())
            //    if (m_current >= 0) {
            //        m_buffers[m_current % m_bufferSize].setFrom(m_access);
            //    }

            final ReentrantLock lock = this.m_lock;
            lock.lock();
            try
            {
                System.out.println("[c]  Q m_current = " + m_current);
                System.out.println("[c]  Q m_bound = " + m_bound);
                System.out.println("[c]  Q m_offset = " + m_offset);
                if (++m_current == m_wrap_at) {
                    m_current = 0;
                    m_offset += m_wrap_at;
                }
                while (m_current == m_bound) {
                    System.out.println("[c]  Q -> m_notFull.await();");
                    m_notFull.await();
                    System.out.println("[c]  Q <- m_notFull.await();");
                }
                m_notEmpty.signal();
                System.out.println("[c]  Q m_current = " + m_current);
                System.out.println("[c]  Q m_bound = " + m_bound);
                System.out.println("[c]  Q m_offset = " + m_offset);
            }
            finally
            {
                lock.unlock();
            }

            return m_current % m_bufferSize;
        }

        /**
         * Get the number of times {@link #forward()} has been called.
         *
         * @return number of calls to {@link #forward()}
         */
        long numForwards() {
            return m_offset + m_current;
        }

        /**
         * @throws InterruptedException if interrupted while waiting
         */
        void finish() throws InterruptedException {
            final ReentrantLock lock = this.m_lock;
            lock.lock();
            try
            {
                m_finishing = true;
                m_notEmpty.signal();
                m_finished.await();
            }
            finally
            {
                lock.unlock();
            }
        }

        interface Serializer {
            void serialize(int previous_head, int head) throws IOException;
            void finish() throws IOException;
        }

        void serializeLoop(final Serializer serializer) throws InterruptedException {

            final ReentrantLock lock = this.m_lock;
            int previous_head = 0;
            int head;
            boolean doFinish;
            while (true) {
                System.out.println("[b] - 0 -");
                System.out.println("[b] - previous_head = " + previous_head);
                lock.lock();
                try {
                    head = m_current;
                    doFinish = m_finishing;
                    System.out.println("[b] - 1 -");
                    System.out.println("[b] - head = " + head);
                    System.out.println("[b] - doFinish = " + doFinish);
                    while (head == previous_head && !doFinish) {
                        System.out.println("[b] - -> m_notEmpty.await();");
                        m_notEmpty.await();
                        System.out.println("[b] - <- m_notEmpty.await();");
                        head = m_current;
                        doFinish = m_finishing;
                        System.out.println("[b] - head = " + head);
                        System.out.println("[b] - doFinish = " + doFinish);
                    }
                } finally {
                    lock.unlock();
                }

                System.out.println("[b] - 2 -");
                // Note that m_previous_head..head maybe empty in case we are finishing
                try {
                    serializer.serialize(previous_head, head);
                    if (doFinish) {
                        serializer.finish();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e); // TODO: how to handle IOException ???
                }

                System.out.println("[b] - 3 -");
                lock.lock();
                try {
                    m_bound = ( head + m_bufferSize ) % m_wrap_at;
                    m_notFull.signal();
                    if (doFinish) {
                        m_finished.signal();
                        return;
                    }
                } finally {
                    lock.unlock();
                }
                System.out.println("[b] - 4 -");

                previous_head = head;
            }
        }
    }


    // --------------------------------------------------------------------
    //
    //   Badger
    //

    class Badger implements Async.Serializer {

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

        Badger(
                final BatchStore store,
                final BufferedAccessRow[] buffers,
                final int maxNumRowsPerBatch,
                final int maxBatchSizeInBytes) {
            m_buffers = buffers;
            m_bufferSize = buffers.length;

            m_maxNumRowsPerBatch = maxNumRowsPerBatch;
            m_maxBatchSizeInBytes = maxBatchSizeInBytes;

            m_writer = store.getWriter();
            m_current_batch = null;

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

        private void writeBufferedRow(final int row) throws IOException {
            System.out.println("[b] Badger.writeBufferedRow( row=" + row + " )");
            final BufferedAccessRow bufferedRow = m_buffers[row];

            // Set the data from the buffer
            for (int col = 0; col < m_accessesToTheCurrentBatch.length; ++col) {
                var access = bufferedRow.getAccess(col);

                // Put all string and var binary data into a heap list/array to be added to the cache later.
                // Does not serialize objects but keeps a reference.

                m_heapCacheBuffers[col].getAccess(m_batchLocalRowIndex).setFrom(access);

                // Write to batch
                m_accessesToTheCurrentBatch[col].setFrom(access);
            }
            ++m_batchLocalRowIndex;

            System.out.println("[b]       localRowIdx:        " + m_batchLocalRowIndex);
            System.out.println("[b]       maxNumRowsPerBatch: " + m_maxNumRowsPerBatch);
            System.out.println("[b]       sizeof batch:       " + m_current_batch.usedSizeFor(m_batchLocalRowIndex));
            System.out.println("[b]       max batch sizeof:   " + m_maxBatchSizeInBytes);
            if (m_batchLocalRowIndex >= m_maxNumRowsPerBatch
                    || m_current_batch.usedSizeFor(m_batchLocalRowIndex) > m_maxBatchSizeInBytes) {
                System.out.println("[b]   switch to new batch");
                // TODO if we have written more data in some columns make sure we do not loose it
                writeCurrentBatch();
                switchToNextBatch();
            }
        }

        private void writeCurrentBatch() throws IOException {
            System.out.println("[b] Badger.writeCurrentBatch");
            ReadBatch readBatch = m_current_batch.close(m_batchLocalRowIndex);
            m_writer.write(readBatch);

            // save data in cache
            for (int col = 0; col < m_store.getSchema().numColumns(); col++) {
                var data = m_heapCacheBuffers[col].getArray();
                if (null != data) {
                    m_heapCache.cacheData((Object[])data, new ColumnDataUniqueId(m_heapCache, col, m_numBatchesWritten));
                }
            }
            readBatch.release();
            m_numBatchesWritten++;
        }

        private void switchToNextBatch() {
            System.out.println("[b] Badger.switchToNextBatch");
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
        public void serialize(final int previous_head, final int head) throws IOException {
            System.out.println("[b] Badger.serialize( previous_head=" + previous_head + ", head=" + head + " )");
            int from = previous_head;
            int to = head;
            if (to < from) {
                from -= m_bufferSize;
                to += m_bufferSize;
            }
            for (int i = from; i < to; ++i) {
                System.out.println( "[b]   writeBufferedRow("+ (i % m_bufferSize) +")  ... i="+i);
                writeBufferedRow(i % m_bufferSize);
            }
        }

        @Override
        public void finish() throws IOException {
            writeCurrentBatch();
            m_writer.close();
        }
    }


    // --------------------------------------------------------------------
    //
    //   BadgerWriteCursor
    //

    static class BadgerWriteCursor implements ColumnarWriteCursor {
        private final BufferedAccessRow[] m_buffers;

        private final BufferedAccessRow m_access;

        private final Async m_queue;

        BadgerWriteCursor(final ColumnarSchema schema, final Async queue) {
            m_queue = queue;
            m_buffers = new BufferedAccessRow[queue.m_bufferSize]; // TODO Async::getBufferSize() instead of field access
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
            System.out.println("[c] BadgerWriteCursor.forward");
            if ( m_current < 0 ) {
                m_current = 0;
                return true;
            }

            m_buffers[m_current].setFrom(m_access);
            try {
                System.out.println("[c]   -> m_queue.forward()");
                m_current = m_queue.forward();
                System.out.println("[c]   <- m_queue.forward()");
                System.out.println("[c]   m_current = " + m_current);
            } catch (InterruptedException e) {
                throw new RuntimeException(e); // TODO: how to handle InterruptedException ???
            }
            return true;
        }

        // TODO rename to finish() ???
        @Override
        public void flush() throws IOException {
            System.out.println("[c] BadgerWriteCursor.flush");
            forward();
            try {
                System.out.println("[c]   -> m_queue.finish()");
                m_queue.finish();
                System.out.println("[c]   <- m_queue.finish()");
            } catch (InterruptedException e) {
                throw new RuntimeException(e); // TODO: how to handle InterruptedException ???
            }
        }

        @Override
        public void close() throws IOException {
            // TODO abort, release resources
        }

        @Override
        public long getNumForwards() {
            return (m_current < 0) ? 0 : m_queue.numForwards();
        }
    }
}
