package org.knime.core.columnar.badger;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.knime.core.columnar.badger.DebugLog.debug;

class DefaultSerializationQueue implements SerializationQueue {
    private final int m_bufferSize;

    private final ReentrantLock m_lock;

    private final Condition m_notEmpty;

    private final Condition m_notFull;

    private final Condition m_flushed;

    private final Condition m_closed;

    /**
     * If an IOException occurs in the serializer thread during serialization or
     * flushing of rows, it is stored here. It should be re-thrown by commit() or
     * flush() as soon as possible.
     * <p>
     * When the queue is close()d, this is set to an IOException indicating that
     * "SerializationQueue is already closed". Invocations of commit() or flush()
     * on the closed queue will then fail with that exception.
     */
    private IOException m_serialization_exception = null;

    /**
     * If an IOException occurs in the serializer thread when closing the
     * serializer, it is stored here. It should be re-thrown by close().
     */
    private IOException m_close_exception = null;

    /**
     * Set by close() to prompt serializeLoop() to close the queue.
     * <p>
     * Also, signals to future close() calls that the queue is already closed and
     * nothing has to be done.
     */
    private boolean m_closing;

    private final int m_wrap_at;

    private int m_current;

    private int m_bound;

    private long m_offset;

    /**
     * Some thread requested rows up to {@code m_flush_request_row} to be
     * flushed. This value is only set by a flushing thread and only ever
     * increases.
     */
    private long m_flush_request_row;

    /**
     * When the serializer thread picks up a flush request and flushes, it
     * sets {@code m_flushed_row} to the index of the last row included in
     * the flush. This value is only set by the serializer thread and only
     * ever increases.
     */
    private long m_flushed_row;

    private Runnable m_interruptSerializerThread;

    /**
     *
     * @param bufferSize number of slots in the queue
     */
    DefaultSerializationQueue(final int bufferSize) {
        m_bufferSize = bufferSize;
        m_wrap_at = 2 * bufferSize;

        m_lock = new ReentrantLock();
        m_notEmpty = m_lock.newCondition();
        m_notFull = m_lock.newCondition();
        m_flushed = m_lock.newCondition();
        m_closed = m_lock.newCondition();

        // initialize indices:

        m_current = 0;
        // After the most recent commit(), the cursor modifies buffers[m_current].
        // Initially, before the first commit(), m_current = 0, so the cursor modifies buffers[0].
        //
        // After commit(), everything up to buffers[m_current-1] is ready to be serialized.
        // So after the first commit(), m_current=1, and buffers[0] is valid. buffers[1] is now modified.
        //
        // When the serializer thread reads head:=m_current, it knows everything up to buffers[head-1] can be serialized.
        // The first index to be serialized is serializer.m_previous_head.
        // After the serializer is done serializing up to buffers[head-1], it sets m_previous_head:=head,
        //   because buffers[head] is the first to be serialized in the next round.

        m_bound = m_bufferSize;
        // commit() increments m_current, and blocks if m_current==m_bound afterward.
        // Otherwise, the access() returned after commit() would write into the range that is currently serializing.
        //
        // If the first serialization has not finished (maybe it was not even triggered yet) when all buffers are filled,
        //   m_current==m_bufferSize, that is access() would return the invalid buffers[m_bufferSize].
        // Technically, m_current should wrap around to 0 then, before the commit() returns.
        // However, from the perspective of the serializer, it would be indistinguishable whether nothing has been
        //   written since the last round, or everything.
        //
        // We disambiguate this by only wrapping to m_current=0 at 2*m_bufferSize,
        //   and taking indices modulo m_bufferSize for reading and writing.
        // That is:
        //   * access() modifies buffers[m_current % m_bufferSize].
        //   * serializer takes buffers from m_previous_head % m_bufferSize to (head-1) % m_bufferSize.

        m_offset = 0;
        // m_offset is used to calculate numRows() as m_offset + m_current.
        // When wrapping at m_wrap_at, m_offset is incremented by m_wrap_at.

        m_flushed_row = -1;
        m_flush_request_row = -1;
    }

    @Override
    public int getBufferSize() {
        return m_bufferSize;
    }

    @Override
    public int commit() throws InterruptedException, IOException {
        final ReentrantLock lock = this.m_lock;
        debug("[q] commit() -- acquiring LOCK");
        lock.lock();
        debug("[q] commit() -- LOCKED");
        try {
            debugPrintIndices();
            ++m_current;
            if (m_current == m_wrap_at) {
                m_current = 0;
                m_offset += m_wrap_at;
            }
            while (!rethrowSerializationException() && m_current == m_bound) {
                debug("[q] commit() -> m_notFull.await();");
                try {
                    m_notFull.await();
                } catch (InterruptedException e) {
                    debug("[q] commit() -- m_notFull.await() interrupted");
                    // roll back index change and re-throw
                    --m_current;
                    if (m_current == -1) {
                        m_current = m_wrap_at - 1;
                        m_offset -= m_wrap_at;
                    }
                    throw e;
                }
                debug("[q] commit() <- m_notFull.await();");
                debugPrintIndices();
            }
        } finally {
            // wake up serializeLoop that might be waiting
            m_notEmpty.signal();
            debug("[q] commit() -- releasing LOCK");
            lock.unlock();
        }

        return m_current % m_bufferSize;
    }

    private void debugPrintIndices() {
        debug("[q]  m_current = {}", m_current);
        debug("[q]  m_bound = {}", m_bound);
        debug("[q]  m_offset = {}", m_offset);
        debug("[q]  m_flushed_row = {}", m_flushed_row);
        debug("[q]  m_flush_request_row = {}", m_flush_request_row);
    }

    @Override
    public long numRows() {
        final ReentrantLock lock = this.m_lock;
        lock.lock();
        try {
            return m_offset + m_current;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Blocks until all rows that were {@link #commit committed} before calling
     * {@code flush()} have been flushed (at least).
     */
    @Override
    public void flush() throws InterruptedException, IOException {
        final ReentrantLock lock = this.m_lock;
        debug("[q] flush() -- acquiring LOCK");
        lock.lock();
        debug("[q] flush() -- LOCKED");
        try {
            debugPrintIndices();
            final long row = m_offset + m_current - 1;
            if (m_flush_request_row < row) {
                m_flush_request_row = row;
            }
            while (!rethrowSerializationException() && m_flushed_row < row) {
                // NB: We will be woken up if the serializer has been flushed.
                // However, we will also be woken up if an exception occurred
                // during serialization.
                m_notEmpty.signal();
                debug("[q] flush() -> m_flushed.await();");
                m_flushed.await();
                debug("[q] flush() <- m_flushed.await();");
                debugPrintIndices();
            }
        } finally {
            debug("[q] flush() -- releasing LOCK");
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        final ReentrantLock lock = this.m_lock;
        debug("[q] close() -- acquiring LOCK");
        lock.lock();
        debug("[q] close() -- LOCKED");
        try {
            if (m_closing) {
                // Do nothing. Queue is already closed (or being closed).
                debug("[q] close() -- already closed");
                return;
            }
            m_closing = true;

            if (m_interruptSerializerThread != null) {
                m_interruptSerializerThread.run();
                m_interruptSerializerThread = null;

                // NB: if m_interruptSerializerThread == null, the serializer
                // may not have been called yet not progressed to a point where
                // it could be waiting on anything. We don't need to interrupt
                // it. It will see m_closing == true, close the serializer, and
                // m_closed.signal().
                //
                // However, we should also be robust wrt the case that
                // serializeLoop() is never called *at all*. Therefore, we do
                // not m_closed.await() unless we are sure that the loop is
                // running.
                //
                // In the worst case, that means:
                //  - close() returns.
                //  - serializeLoop() is called later
                //  - it sees m_closing and immediately closes the serializer
                //  - closing the serializer throws an IOException which is
                //    silently swallowed because it is too late for close() to
                //    see it.
                //
                debug("[q] close() -> m_closed.await()");
                m_closed.await();
                debug("[q] close() <- m_closed.await()");
            }

            if (m_close_exception != null) {
                debug("[q] close() -- rethrowing m_close_exception");
                throw m_close_exception;
            }
        } catch (InterruptedException e) {
            if (m_close_exception != null) {
                m_close_exception.addSuppressed(e);
                throw m_close_exception;
            } else {
                throw new IOException(e);
            }
        } finally {
            debug("[q] close() -- releasing LOCK");
            lock.unlock();
        }
    }

    /**
     * Re-throws {@code m_serialization_exception} if set,
     * or returns {@code false} if {@code m_serialization_exception==null}.
     */
    private boolean rethrowSerializationException() throws IOException {
        if (m_serialization_exception != null) {
            debug("[q] rethrowSerializationException() -- throwing {}", m_serialization_exception);
            throw m_serialization_exception;
        }
        return false;
    }

    /**
     * Creates a {@code Runnable} that will interrupt the thread calling this method.
     */
    private void createSerializerThreadInterrupter() {
        final ReentrantLock lock = this.m_lock;
        lock.lock();
        try {
            m_interruptSerializerThread = Thread.currentThread()::interrupt;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void serializeLoop(final Serializer serializer) {

        debug("[s] - serializeLoop start");
        createSerializerThreadInterrupter();

        // The index of the last row that has been serialized. This value is updated
        // after every invocation of serializer.serialize(from, to).
        long serialized_row = -1;

        // The index of the last row to be flushed.
        // (This is a local copy of m_flush_request_row for use outside locked sections).
        long flush_request;

        final ReentrantLock lock = this.m_lock;
        int previous_head = 0;
        int head;
        boolean doClose;
        boolean doFlush;
        while (true) {
            debug("[s] - 0 -");
            debug("[s] - previous_head = {}", previous_head);

            // -----------------------------------------------------------------
            // (1) Wait until either
            //     new data is available, or
            //     flush() has been requested, or
            //     the queue is being closed.
            //
            debug("[s] - 1 -");
            debug("[s] - acquiring LOCK");
            lock.lock();
            debug("[s] - LOCKED");
            try {
                head = m_current;
                flush_request = m_flush_request_row;
                doClose = m_closing;
                doFlush = m_flush_request_row > m_flushed_row;
                debug("[s] - head = {}", head);
                debug("[s] - flush_request = {}", flush_request);
                debug("[s] - doClose = {}", doClose);
                debug("[s] - doFlush = {}", doFlush);
                while (head == previous_head && !doFlush && !doClose) {
                    try {
                        // NB: We will be woken up if a new entry is committed to the queue. However, we will also
                        // be woken up if flush() or close() is called. In this case, the queue actually may still be
                        // empty when we exit the while loop.
                        debug("[s] - -> m_notEmpty.await();");
                        m_notEmpty.await();
                        debug("[s] - <- m_notEmpty.await();");
                    } catch (InterruptedException ex) {
                        debug("[s] - m_notEmpty.await() interrupted");
                        // If we are interrupted (for any reason), close the queue.
                        m_closing = true;
                    }
                    head = m_current;
                    flush_request = m_flush_request_row;
                    doClose = m_closing;
                    doFlush = m_flush_request_row > m_flushed_row;
                    debug("[s] - head = {}", head);
                    debug("[s] - flush_request = {}", flush_request);
                    debug("[s] - doClose = {}", doClose);
                    debug("[s] - doFlush = {}", doFlush);
                }
            } finally {
                debug("[s] - releasing LOCK");
                lock.unlock();
            }

            // -----------------------------------------------------------------
            // (2) If the queue is being closed, don't bother with the
            //     serialization below. Just release resources and abort.
            //
            debug("[s] - 2 -");
            if (doClose) {
                // If the queue is being closed, don't bother with the serialization below.
                // Just release resources and abort.
                try {
                    debug("[s] - serializer.close()");
                    serializer.close();
                } catch (IOException e) {
                    m_close_exception = e;

                    // If we are closing because of an earlier IOException (below) there is no explicit close().
                    // We concatenate the exceptions so that they will be rethrown out of commit() or flush().
                    if (m_serialization_exception != null) {
                        m_close_exception.addSuppressed(m_serialization_exception);
                        m_serialization_exception = m_close_exception;
                    }
                }

                // Future invocations of commit() or flush() on the closed queue will throw this exception.
                if (m_serialization_exception == null) {
                    m_serialization_exception = new IOException("SerializationQueue is already closed");
                }

                debug("[s] - acquiring LOCK");
                lock.lock();
                debug("[s] - LOCKED");
                try {
                    m_closed.signal();
                    m_notFull.signal();
                    m_flushed.signalAll();
                } finally {
                    debug("[s] - releasing LOCK");
                    lock.unlock();
                }

                debug("[s] - serializeLoop exit");
                return;
            }

            // -----------------------------------------------------------------
            // (3) Serialize pending data and flush the serializer if requested.
            //
            debug("[s] - 3 -");
            final boolean flushSerializer;
            try {
                // Note that [from, to) maybe empty in case we are flushing
                int from = previous_head;
                int to = head;
                if (to < from) {
                    from -= m_bufferSize;
                    to += m_bufferSize;
                }
                debug("[s] - serialize({}, {})", from, to);
                serializer.serialize(from, to);
                serialized_row += (to - from);
                flushSerializer = doFlush && flush_request <= serialized_row;
                if (flushSerializer) {
                    debug("[s] - serializer.flush()");
                    serializer.flush();
                }
            } catch (IOException e) {
                debug("[s] - caught IOException");

                // If an IOException occurs, close the queue and make sure the
                // IOException is re-thrown.
                m_closing = true;
                m_serialization_exception = e;

                // Continue to the next iteration of the while() loop
                // which will skip right to the if(doClose) block above.
                continue;
            } catch (InterruptedException e) {
                // If we are interrupted (for any reason), close the queue.
                m_closing = true;

                // Continue to the next iteration of the while() loop
                // which will skip right to the if(doClose) block above.
                continue;
            }

            // -----------------------------------------------------------------

            // (4) Update m_bound and wake up any thread potentially waiting in
            //     commit(). If we did flush the serializer, wake up all threads
            //     waiting in flush() so they can check whether flushing already
            //     proceeded to the row they were waiting for.
            debug("[s] - 4 -");
            debug("[s] - acquiring LOCK");
            lock.lock();
            debug("[s] - LOCKED");
            try {
                m_bound = (head + m_bufferSize) % m_wrap_at;
                debug("[s] - m_bound = {}", m_bound);
                m_notFull.signal();
                if (flushSerializer) {
                    m_flushed_row = serialized_row;
                    m_flushed.signalAll();
                }
            } finally {
                debug("[s] - releasing LOCK");
                lock.unlock();
            }

            previous_head = head;
        }
    }
}
