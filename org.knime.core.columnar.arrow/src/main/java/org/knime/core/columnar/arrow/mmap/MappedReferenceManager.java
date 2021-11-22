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
 *   Mar 31, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow.mmap;

import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OwnershipTransferResult;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.MemoryUtil;

import com.google.common.base.Preconditions;

/**
 * A {@link ReferenceManager} for a memory-mapped buffer. Keeps track of a reference count and closes the resources when
 * the reference count drops to 0.
 *
 * When the reference count drops to 0..
 * <ul>
 * <li>..MappedMessageSerializer#removeBatch(MappedReferenceManager) is called</li>
 * <li>..the only reference to the {@link MappedByteBuffer} is deleted.</li>
 * </ul>
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class MappedReferenceManager implements ReferenceManager {

    private final AtomicInteger m_refCount;

    private final long m_size;

    private final MappedArrowBatchKey m_key;

    // We need to keep a reference to the MappedByteBuffer to keep it mapped
    private MappedByteBuffer m_mappedBuffer;

    /**
     * Create a new {@link ReferenceManager} for memory-mapped buffers with a reference count of 1.
     *
     * @param size the size of the mapped memory
     * @param key the key for the batch that belongs to this reference manager. Used to remove it from the map in
     *            {@link MappedMessageSerializer}.
     * @param mappedBuffer the mapped buffer. This object holds a reference to it until the reference count drops to 0
     */
    MappedReferenceManager(final long size, final MappedArrowBatchKey key, final MappedByteBuffer mappedBuffer) {
        m_size = size;
        m_key = key;
        m_mappedBuffer = mappedBuffer;
        m_refCount = new AtomicInteger(1);
    }

    MappedArrowBatchKey getKey() {
        return m_key;
    }

    @Override
    public int getRefCount() {
        return m_refCount.get();
    }

    @Override
    public boolean release() {
        return release(1);
    }

    @Override
    public boolean release(final int decrement) {
        final int refCount = m_refCount.addAndGet(-decrement);
        if (refCount == 0) {
            // Remove the remembered batch
            // Note that the batch is not removed if the reference count goes up again before this is finished
            if (MappedMessageSerializer.removeBatch(this)) {

                // Close the mapping explicitly by calling the cleaner of the buffer
                MemoryUtil.UNSAFE.invokeCleaner(m_mappedBuffer);
                m_mappedBuffer = null;
                return true;
            }
            return false;
        }
        Preconditions.checkState(refCount >= 0, "RefCnt has gone negative");
        return false;
    }

    @Override
    public void retain() {
        retain(1);
    }

    @Override
    public void retain(final int increment) {
        // NB: m_refCount can be 0 here.
        // This happens if this is released and at the same time deserialized again from the MappedMessageSerializer.
        // MappedMessageSerializer#deserializeBatch acquires the lock first and calls retain.
        // MappedMessageSerializer#removeBatch will return "false" because the ref count is not 0 anymore and m_mappedBuffer
        // will not be cleaned and set to null.
        m_refCount.getAndAdd(increment);
    }

    @Override
    public BufferAllocator getAllocator() {
        throw new UnsupportedOperationException("A mapped arrow buffer does not have an allocator.");
    }

    @Override
    public ArrowBuf retain(final ArrowBuf srcBuffer, final BufferAllocator targetAllocator) {
        Preconditions.checkArgument(srcBuffer.getReferenceManager() == this,
            "Can only retain for buffers with this reference manager.");
        // Mapped buffers are not associated with any allocator
        // Therefore, it does not have to be associated with targetAllocator
        // -> We just retain and return the buffer
        retain();
        return srcBuffer;
    }

    @Override
    public ArrowBuf deriveBuffer(final ArrowBuf sourceBuffer, final long index, final long length) {
        return new ArrowBuf(this, null, length, sourceBuffer.memoryAddress() + index);
    }

    @Override
    public OwnershipTransferResult transferOwnership(final ArrowBuf sourceBuffer,
        final BufferAllocator targetAllocator) {
        throw new UnsupportedOperationException("The ownership of a mapped arrow buffer cannot be transferred.");
    }

    @Override
    public long getSize() {
        return m_size;
    }

    @Override
    public long getAccountedSize() {
        return getSize();
    }
}
