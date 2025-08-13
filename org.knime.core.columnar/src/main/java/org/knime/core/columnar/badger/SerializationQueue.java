package org.knime.core.columnar.badger;

import java.io.Closeable;
import java.io.IOException;

/**
 * Coordinating between a producer that puts entries-to-serialize into slots of the queue and a consumer
 * ({@link Serializer}) that serializes and removes completed entries.
 * <p>
 * SerializationQueue only manages slot indices and synchronization. It does not manage the actual buffer/slots
 * holding the entries.
 */
interface SerializationQueue extends Closeable {
    interface Serializer {
        /**
         * Serialize slots with indices (modulo {@link #getBufferSize() buffer size}) in the given range
         * {@code [from, to)}.
         * <p>
         * The indices are in the range {@code 0 ≤ from ≤ to < 2 * bufferSize}. Indices should be taken modulo
         * {@link #getBufferSize()}. (It always holds that {@code from ≤ to}, even when the range modulo buffer size
         * wraps around the end of the buffer.)
         *
         * @param from first index to serialize (inclusive, modulo buffer size)
         * @param to   last index to serialize (exclusive, modulo buffer size)
         * @throws IOException          if an I/O error occurs during serialization
         * @throws InterruptedException if the calling thread is interrupted during serialization
         */
        void serialize(int from, int to) throws IOException, InterruptedException;

        void finish() throws IOException;

        void flush() throws IOException, InterruptedException;

        void close() throws IOException;
    }

    /**
     * This worker method runs in a separate thread and serializes entries when they become available.
     *
     * @param serializer
     */
    void serializeLoop(Serializer serializer);

    /**
     * Commit the current entry and return the index of the next entry to be written.
     * Note that the first entry to be written to (before the first {@code commit}) is at index 0.
     * <p>
     * If there is no free slot for the next entry, {@code commit()} blocks until the {@link #serializeLoop} makes
     * progress and a free slot becomes available.
     * <p>
     *
     * @return index of buffer to modify
     * @throws InterruptedException if interrupted while waiting
     * @throws IOException          if a serializer has failed (in a separate thread) since the last call to commit()
     */
    int commit() throws InterruptedException, IOException;

    /**
     * Blocks until all queued entries have been processed by the {@link Serializer}.
     * <p>
     * When {@code flush()} returns {@link Serializer#serialize} has run on all entries, and
     * {@link Serializer#flush} has been called.
     * <p>
     * TODO (TP): Clarify concurrency semantics. flush() may be called from any
     *   thread nd will block (only) that thread until all entries queued at the
     *   time when flush() was called have been serialized. Other entries might
     *   have been enqueued in the meantime by the committer thread. (For now
     *   assuming that there is only one committer thread...)
     *
     *
     * @throws InterruptedException if interrupted while waiting
     * @throws IOException          if a serializer has failed (in a separate thread) since the last call to commit()
     */
    void flush() throws InterruptedException, IOException;

    /**
     * Waits for all queued serializations to finish.
     * <p>
     * When {@code finish()} returns {@link Serializer#serialize} has run on all entries, and
     * {@link Serializer#finish} has been called.
     * <p>
     * No further entries are accepted after {@code finish()}.
     *
     * @throws InterruptedException if interrupted while waiting
     * @throws IOException          if a serializer has failed (in a separate thread) since the last call to commit()
     */
    default void finish() throws InterruptedException, IOException {
        try {
            flush();
        } finally {
            close();
        }
    }

    /**
     * @return the number of slots
     */
    int getBufferSize();

    /**
     * Get the number of times {@link #commit()} has been called.
     *
     * @return number of calls to {@link #commit()}
     */
    long numRows();
}
