package org.knime.core.columnar.badger;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.knime.core.columnar.badger.DebugLog.debug;

class AsyncQueue implements SerializationQueue {
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

    private Serializer m_serializer;

    private Runnable m_interruptSerializerThread;

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
        // After the most recent commit(), the cursor modifies buffers[m_current].
        // Initially, before the first commit(), m_current = 0, so the cursor modifies m_buffers[0].
        //
        // After commit(), everything up to m_buffers[m_current-1] is ready to be serialized.
        // So after the first commit(), m_current=1, and m_buffers[0] is valid. m_buffers[1] is now modified.
        //
        // When the serializer thread reads head:=m_current, it knows everything up to m_buffers[head-1] can be serialized.
        // The first index to be serialized is serializer.m_previous_head.
        // After the serializer is done serializing up to m_buffers[head-1], it sets m_previous_head:=head,
        //   because m_buffers[head] is the first to be serialized in the next round.

        m_bound = m_bufferSize;
        // commit() increments m_current, and blocks if m_current==m_bound afterwards.
        // Otherwise, the access() returned after commit() would write into the range that is currently serializing.
        //
        // If the first serialization has not finished (maybe it was not even triggered yet) when m_buffers is filled,
        //   m_current==m_bufferSize, that is access() would return the invalid m_buffers[m_bufferSize].
        // Technically, m_current should wrap around to 0 then, before the commit() returns.
        // However, from the perspective of the serializer, it would be indistinguishable whether nothing has been
        //   written since the last round, or everything.
        //
        // We disambiguate this by only wrapping to m_current=0 at 2*m_bufferSize,
        //   and taking indices modulo m_bufferSize for reading and writing.
        // That is:
        //   * access() modifies m_buffers[m_current % m_bufferSize].
        //   * serializer takes buffers from m_previous_head % m_bufferSize to (head-1) % m_bufferSize.

        m_offset = 0;
        // m_offset is used to calculate numRows() as m_offset + m_current.
        // When wrapping at m_wrap_at, m_offset is incremented by m_wrap_at.
    }

    @Override
    public int getBufferSize() {
        return m_bufferSize;
    }

    @Override
    public int commit() throws InterruptedException, IOException {

        // TODO: throw Exception if finishing==true?
        //       @throws IllegalStateException if called after {@link #finish}

        final ReentrantLock lock = this.m_lock;
        debug("[c] ACQUIRING LOCK IN COMMIT");
        lock.lock();
        debug("[c] LOCKED IN COMMIT");
        try {
            debug("[c]  Q m_current = {}", m_current);
            debug("[c]  Q m_bound = {}", m_bound);
            debug("[c]  Q m_offset = {}", m_offset);
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
            debug("[c]  Q m_current = {}", m_current);
            debug("[c]  Q m_bound = {}", m_bound);
            debug("[c]  Q m_offset = {}", m_offset);
        } finally {
            debug("[c] UNLOCKING IN COMMIT");
            lock.unlock();
        }

        rethrowExceptionInErrorCase();

        return m_current % m_bufferSize;
    }

    @Override
    public long numRows() {
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

            // TODO (TP): Definitely call m_serializer.flush() here, even if isQueueEmpty() == true above.
            //            Not sure about what should happen if m_exception != null.

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

        // TODO (TP): if case m_exception != null, the serialization loop
        //            has already exited and will signal() no more conditions.
        //            1. We should check for m_exception != null here and rethrow.
        //            2. We should also make sure to m_serializer.close()

        final ReentrantLock lock = this.m_lock;
        lock.lock();
        try {
            // TODO (TP): We can use the following variant if we make sure to use m_finished.signalAll()
            //                     if (!m_finishing) {
            //                         m_finishing = true;
            //                         m_notEmpty.signal();
            //                     }
            //                     m_finished.await(); // is reached definitely if errors occurred
            //            We will additionally need a boolean m_isFinished
            //            flag, in case a thread calls finish() again when
            //            finish() is already done once.
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

    @Override
    public void close() throws IOException {


        final ReentrantLock lock = this.m_lock;
        debug("[b] Close -- acquiring LOCK");
        lock.lock();

        // TODO (TP): set m_closing = true; here? (before interrupting?)

        // Note that we have to cause the serialization loop to be interrupted before calling `m_queue.close()`.
        // Otherwise the serialization loop can get stuck in `m_notEmpty.await()` while `m_queue.close()` is waiting
        // on the `m_closed` condition.
        if (m_interruptSerializerThread != null) {
            m_interruptSerializerThread.run();
            m_interruptSerializerThread = null;
        }

        debug("[b] Close -- LOCKING");
        try {
            // In case m_exception != null, the serialization loop has
            // already exited. We don't need to await m_closed (and should
            // not, because it will never be signaled).
            //
            // TODO (TP): The !m_finishing condition is maybe a bug?
            //            If a finish() is currently ongoing, we probably
            //            not just ignore it and m_serializer.close()?
            //            On master, I think before queue.close, the
            //            serializer thread is interrupted.
            if (m_exception == null && !m_finishing) {
                debug("[b] Close -- waiting for closed signal");
                m_closing = true;
                m_notEmpty.signal();
                m_closed.await();
            }

            m_serializer.close();
            debug("[b]  close AsyncQueue");
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } finally {
            lock.unlock();
            debug("[b] Close -- UNLOCKING");
        }
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

    private Runnable createSerializerThreadInterrupter() {
        final ReentrantLock lock = this.m_lock;
        lock.lock();
        try {
            return Thread.currentThread()::interrupt;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void serializeLoop(final Serializer serializer) {
        m_serializer = serializer;
        m_interruptSerializerThread = createSerializerThreadInterrupter();

        final ReentrantLock lock = this.m_lock;
        int previous_head = 0;
        int head;
        boolean doClose;
        boolean doFinish;
        while (true) {
            debug("[b] - 0 -");
            debug("[b] - previous_head = {}", previous_head);
            debug("[b] ACQUIRING LOCK IN LOOP");
            lock.lock();
            debug("[b] LOCKED IN LOOP");
            try {
                head = m_current;
                doClose = m_closing;
                doFinish = m_finishing;
                debug("[b] - 1 -");
                debug("[b] - head = {}", head);
                debug("[b] - doFinish = {}", doFinish);
                while (head == previous_head && !doFinish && !doClose) {
                    debug("[b] - -> m_notEmpty.await();");
                    try {
                        m_notEmpty.await();
                    } catch (InterruptedException ex) {
                        // in case of an exception, we remember it and quit the serialization loop
                        m_exception = ex;
                        // to unblock commit() we need to claim that there's more space, but it'll rethrow the exception
                        m_notFull.signal();
                        m_closed.signal();
                        m_finished.signal();
                        debug("[b] ERROR EXIT a) because of {}", ex.getMessage());
                        return;
                    }
                    debug("[b] - <- m_notEmpty.await();");
                    head = m_current;
                    doClose = m_closing;
                    doFinish = m_finishing;
                    debug("[b] - head = {}", head);
                    debug("[b] - doFinish = {}", doFinish);
                }
            } finally {
                debug("[b] UNLOCKING IN LOOP");
                lock.unlock();
            }

            debug("[b] - 2 -");
            // Note that previous_head..head maybe empty in case we are finishing
            try {
                int from = previous_head;
                int to = head;
                if (to < from) {
                    from -= m_bufferSize;
                    to += m_bufferSize;
                }
                serializer.serialize(from, to);
                if (doFinish) {
                    serializer.finish();
                }
            } catch (Exception e) {
                // in case of an exception, we remember it and quit the serialization loop
                m_exception = e;
                debug("[b] LOOP ERROR EXIT -- acquiring LOCK");
                lock.lock();
                debug("[b] LOOP ERROR EXIT -- LOCKING");
                // to unblock commit() we need to claim that there's more space, but it'll rethrow the exception
                m_notFull.signal();
                m_closed.signal();
                m_finished.signal();
                debug("[b] LOOP ERROR EXIT -- UNLOCKING");
                lock.unlock();
                debug("[b] ERROR EXIT b) because of {}", e.getMessage());
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
}
