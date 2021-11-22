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
 *   Mar 4, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow.mmap;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowMessage;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import com.google.common.util.concurrent.Striped;

/**
 * This class provides utility methods to deserialize {@link ArrowRecordBatch} and {@link ArrowDictionaryBatch} using
 * memory-mapping.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class MappedMessageSerializer {

    private static final Map<MappedArrowBatchKey, ArrowMessage> MAPPED_BATCHES = new ConcurrentHashMap<>();

    private static final Striped<Lock> LOCKS = Striped.lock(128);

    private MappedMessageSerializer() {
    }

    /** DO NOT USE: ONLY FOR TESTING */
    static void assertAllClosed() {
        if (!MAPPED_BATCHES.isEmpty()) {
            MAPPED_BATCHES.clear();
            throw new AssertionError("Mapped Message BATCHES not empty.");
        }
    }

    /**
     * Deserialize the record batch from the channel and memory-map its data. If the record batch data is already mapped
     * inside the memory a reference to this mapping is returned.
     *
     * @param in the channel to read from
     * @param offset the offset in the channel
     * @return the record batch with buffers pointing to the memory-mapped region. Must be closed when the caller is
     *         finished with it.
     * @throws IOException if reading the record batch failed
     */
    public static ArrowRecordBatch deserializeRecordBatch(final MappableReadChannel in, final long offset)
        throws IOException {
        return deserializeBatch(in, offset, MessageHeader.RecordBatch);
    }

    /**
     * Deserialize the dictionary batch from the channel and memory-map its data. If the batch data is already mapped
     * inside the memory a reference to this mapping is returned.
     *
     * @param in the channel to read from
     * @param offset the offset in the channel
     * @return the dictionary batch with buffers pointing to the memory-mapped region. Must be closed when the caller is
     *         finished with it.
     * @throws IOException if reading the dictionary batch failed
     */
    public static ArrowDictionaryBatch deserializeDictionaryBatch(final MappableReadChannel in, final long offset)
        throws IOException {
        return deserializeBatch(in, offset, MessageHeader.DictionaryBatch);
    }

    /** Remove the batch associated with the reference manager from the map. Returns true if the batch was removed. */
    @SuppressWarnings("resource") // The record batch does not need to be closed. This is only called when the body buffer is closed
    static boolean removeBatch(final MappedReferenceManager refMan) {
        final MappedArrowBatchKey key = refMan.getKey();
        final Lock lock = LOCKS.get(key);
        lock.lock();
        try {
            // RefCount can be > 0 if deserialize(Record|Dictionary)Batch acquired the lock after it dropped to 0 but before the lock was acquired here
            // In this case we do not want to remove the batch because it is used again
            if (refMan.getRefCount() == 0) {
                final ArrowMessage batch = MAPPED_BATCHES.remove(key);
                if (batch == null) {
                    // The batch was removed already
                    // A consumer of the batch called retain after the reference count dropped to 0. This is not allowed.
                    throw new IllegalStateException("Tried to release a batch that has been released already.");
                }
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    /** Deserialize a record batch or dictionary batch from the input channel */
    @SuppressWarnings("resource")
    private static <M extends ArrowMessage> M deserializeBatch(final MappableReadChannel in, final long offset,
        final byte expectedHeaderType) throws IOException {
        final MappedArrowBatchKey key = new MappedArrowBatchKey(in.getFile(), offset);

        // Acquire the lock for this file+offset combination
        final Lock lock = LOCKS.get(key);
        lock.lock();

        try {
            ArrowMessage batch = MAPPED_BATCHES.get(key);
            if (batch != null) {
                // The batch is already mapped. We just need to wrap and retain it again
                return wrapM(batch, true);
            } else {
                // Read the message
                in.setPosition(offset);
                final MessageMetadataResult result = MessageSerializer.readMessage(in);

                // Check that we read the expected message
                if (result == null) {
                    throw new IOException(
                        "Unexpected end of input when reading a " + MessageHeader.name(expectedHeaderType));
                }
                if (result.getMessage().headerType() != expectedHeaderType) {
                    throw new IOException("Expected " + MessageHeader.name(expectedHeaderType) + " but header was "
                        + MessageHeader.name(result.getMessage().headerType()));
                }

                // Get the location and size of the body
                final long bodyLength = result.getMessageBodyLength();

                // Map the body to memory
                final ArrowBuf bodyBuffer = mapMessageBody(key, in, bodyLength);

                // Make sure the buffer is not closed before the batch is added. (For empty buffers)
                bodyBuffer.getReferenceManager().retain();

                // Deserialize into ArrowRecordBatch or ArrowDictionaryBatch
                if (expectedHeaderType == MessageHeader.RecordBatch) {
                    batch = MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
                } else if (expectedHeaderType == MessageHeader.DictionaryBatch) {
                    batch = MessageSerializer.deserializeDictionaryBatch(result.getMessage(), bodyBuffer);
                } else {
                    throw new IllegalStateException(
                        "The expected header must be a RecordBatch or DictionaryBatch. Got '"
                            + MessageHeader.name(expectedHeaderType) + "'.");
                }
                // Remember the batch
                MAPPED_BATCHES.put(key, batch);
                bodyBuffer.getReferenceManager().release();
                return wrapM(batch, false);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Map the given channel into memory. Associate the resulting buffer with the key. */
    private static ArrowBuf mapMessageBody(final MappedArrowBatchKey key, final MappableReadChannel in,
        final long bodyLength) throws IOException {
        final MappedByteBuffer mappedBopy = in.map(MapMode.READ_ONLY, bodyLength);
        final long address = MemoryUtil.getByteBufferAddress(mappedBopy);
        final MappedReferenceManager referenceManager = new MappedReferenceManager(bodyLength, key, mappedBopy);
        return new ArrowBuf(referenceManager, null, bodyLength, address);
    }

    @SuppressWarnings("unchecked")
    private static <M extends ArrowMessage> M wrapM(final ArrowMessage batch, final boolean retain) {
        if (batch instanceof ArrowRecordBatch) {
            return (M)wrap((ArrowRecordBatch)batch, retain);
        } else if (batch instanceof ArrowDictionaryBatch) {
            return (M)wrap((ArrowDictionaryBatch)batch, retain);
        }
        throw new IllegalStateException(
            "Trying to wrap unsupported message of class " + batch.getClass().getSimpleName());
    }

    /** Wraps the given batch into another one. The returned batch can be closed without closing the given batch */
    private static ArrowRecordBatch wrap(final ArrowRecordBatch batch, final boolean retain) {
        // NB: The ArrowRecordBatch constructor calls retain on all buffers
        final ArrowRecordBatch b =
            new ArrowRecordBatch(batch.getLength(), batch.getNodes(), batch.getBuffers(), batch.getBodyCompression());
        if (!retain) {
            for (final ArrowBuf buf : b.getBuffers()) {
                buf.getReferenceManager().release();
            }
        }
        return b;
    }

    /** Wraps the given batch into another one. The returned batch can be closed without closing the given batch */
    @SuppressWarnings("resource")
    private static ArrowDictionaryBatch wrap(final ArrowDictionaryBatch batch, final boolean retain) {
        final ArrowRecordBatch dictionary = wrap(batch.getDictionary(), retain);
        return new ArrowDictionaryBatch(batch.getDictionaryId(), dictionary, batch.isDelta());
    }
}