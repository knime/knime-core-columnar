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

import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowMessage;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

/**
 * This class provides utility methods to deserialize {@link ArrowRecordBatch} and {@link ArrowDictionaryBatch} using
 * memory-mapping.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class MappedMessageSerializer {

    private static final Map<MappedArrowBatchKey, ArrowMessage> MAPPED_BATCHES = new ConcurrentHashMap<>();

    private static final Map<Object, Object> LOCKS = new ConcurrentHashMap<>();

    /** Lock to synchronize all operations with an equal Object o */
    private static Object getLockFor(final Object o) {
        return LOCKS.computeIfAbsent(o, k -> new Object());
    }

    private MappedMessageSerializer() {
    }

    /** DO NOT USE: ONLY FOR TESTING */
    static void assertAllClosed() {
        if (!LOCKS.isEmpty()) {
            LOCKS.clear();
            throw new AssertionError("Mapped Message LOCKS not empty.");
        }
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
    @SuppressWarnings("resource")
    public static ArrowRecordBatch deserializeRecordBatch(final MappableReadChannel in, final long offset)
        throws IOException {
        final MappedArrowBatchKey key = new MappedArrowBatchKey(in.getFile(), offset);
        synchronized (getLockFor(key)) {
            final ArrowMessage batch = MAPPED_BATCHES.get(key);
            if (batch != null) {
                return wrap((ArrowRecordBatch)batch, true);
            } else {
                // We have to read it
                in.setPosition(offset);
                final MessageMetadataResult result = MessageSerializer.readMessage(in);

                // Check that we read a Record batch
                if (result == null) {
                    throw new IOException("Unexpected end of input when reading a RecordBatch");
                }
                if (result.getMessage().headerType() != MessageHeader.RecordBatch) {
                    throw new IOException("Expected RecordBatch but header was " + result.getMessage().headerType());
                }

                // Get the location and size of the body
                final long bodyLength = result.getMessageBodyLength();

                final ArrowBuf bodyBuffer = mapMessageBody(key, in, bodyLength);
                // Make sure the buffer is not closed before the batch is added. (For empty buffers)
                bodyBuffer.getReferenceManager().retain();
                final ArrowRecordBatch recordBatch =
                    MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
                MAPPED_BATCHES.put(key, recordBatch);
                bodyBuffer.getReferenceManager().release();
                return wrap(recordBatch, false);
            }
        }
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
    @SuppressWarnings("resource")
    public static ArrowDictionaryBatch deserializeDictionaryBatch(final MappableReadChannel in, final long offset)
        throws IOException {
        final MappedArrowBatchKey key = new MappedArrowBatchKey(in.getFile(), offset);
        synchronized (getLockFor(key)) {
            final ArrowMessage batch = MAPPED_BATCHES.get(key);
            if (batch != null) {
                return wrap((ArrowDictionaryBatch)batch, true);
            } else {
                // We have to read it
                in.setPosition(offset);
                final MessageMetadataResult result = MessageSerializer.readMessage(in);

                // Check that we read a Record batch
                if (result == null) {
                    throw new IOException("Unexpected end of input when reading a RecordBatch");
                }
                if (result.getMessage().headerType() != MessageHeader.DictionaryBatch) {
                    throw new IOException(
                        "Expected DictionaryBatch but header was " + result.getMessage().headerType());
                }

                // Get the location and size of the body
                final long bodyLength = result.getMessageBodyLength();

                final ArrowBuf bodyBuffer = mapMessageBody(key, in, bodyLength);
                // Make sure the buffer is not closed before the batch is added. (For empty buffers)
                bodyBuffer.getReferenceManager().retain();
                final ArrowDictionaryBatch dictBatch =
                    MessageSerializer.deserializeDictionaryBatch(result.getMessage(), bodyBuffer);
                MAPPED_BATCHES.put(key, dictBatch);
                bodyBuffer.getReferenceManager().release();
                return wrap(dictBatch, false);
            }
        }
    }

    /** Remove the batch associated with the reference manager from the map. Returns true if the batch was removed. */
    @SuppressWarnings("resource") // The record batch does not need to be closed. This is only called when the body buffer is closed
    static boolean removeBatch(final MappedReferenceManager refMan) {
        final MappedArrowBatchKey key = refMan.getKey();
        synchronized (getLockFor(key)) {
            // RefCount can be > 0 if deserialize(Record|Dictionary)Batch acquired the lock after it dropped to 0 but before the lock was acquired here
            // In this case we do not want to remove the batch because it is used again
            if (refMan.getRefCount() == 0) {
                MAPPED_BATCHES.remove(key);
                LOCKS.remove(key);
                return true;
            } else {
                return false;
            }
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

    /** Wraps the given batch into another one. The retuned batch can be closed without closing the given batch */
    private static ArrowRecordBatch wrap(final ArrowRecordBatch batch, final boolean retain) {
        final ArrowRecordBatch b =
            new ArrowRecordBatch(batch.getLength(), batch.getNodes(), batch.getBuffers(), batch.getBodyCompression());
        if (!retain) {
            for (final ArrowBuf buf : b.getBuffers()) {
                buf.getReferenceManager().release();
            }
        }
        return b;
    }

    /** Wraps the given batch into another one. The retuned batch can be closed without closing the given batch */
    @SuppressWarnings("resource")
    private static ArrowDictionaryBatch wrap(final ArrowDictionaryBatch batch, final boolean retain) {
        final ArrowRecordBatch dictionary = wrap(batch.getDictionary(), retain);
        return new ArrowDictionaryBatch(batch.getDictionaryId(), dictionary, batch.isDelta());
    }
}