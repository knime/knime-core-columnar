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
 */
package org.knime.core.columnar.arrow.data;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.data.BinarySupplData;
import org.knime.core.columnar.phantom.CloseableCloser;

public class ArrowBinarySupplementData<C extends ArrowData<?>> implements BinarySupplData<C>, ArrowData<StructVector> {

    private AtomicInteger m_ref = new AtomicInteger(1);

    private final ArrowVarBinaryData m_binarySuppl;

    private C m_chunk;

    private final StructVector m_vector;

    CloseableCloser m_vectorCloser;

    public static <C extends ArrowData<?>> ArrowBinarySupplementData createEmpty(final BufferAllocator allocator,
        final C chunk) {
        final ArrowBinarySupplementData data = new ArrowBinarySupplementData(allocator, chunk);
        data.m_vectorCloser = CloseableCloser.create(data, data.m_vector, "Arrow Binary Supplement Data");
        return data;
    }

    public static <C extends ArrowData<?>> ArrowBinarySupplementData wrap(final StructVector vector, final C chunk) {
        final ArrowBinarySupplementData data = new ArrowBinarySupplementData(vector, chunk);
        data.m_vectorCloser = CloseableCloser.create(data, data.m_vector, "Arrow Binary Supplement Data");
        return data;
    }

    private ArrowBinarySupplementData(final BufferAllocator allocator, final C chunk) {
        m_chunk = chunk;
        m_binarySuppl = ArrowVarBinaryData.createEmpty(allocator);
        final CustomStructVector vector = new CustomStructVector("BinarySuppl", allocator);

        vector.putChild("Data", m_chunk.get());

        m_vector = vector;
    }

    private ArrowBinarySupplementData(final StructVector vector, final C chunk) {
        m_vector = vector;
        m_chunk = chunk;
        m_binarySuppl = ArrowVarBinaryData.wrap((VarBinaryVector)vector.getChildByOrdinal(1));
    }

    @Override
    public void setMissing(final int index) {
        m_chunk.setMissing(index);
    }

    @Override
    public void ensureCapacity(final int chunkSize) {
        m_chunk.ensureCapacity(chunkSize);
    }

    @Override
    public C getChunk() {
        return m_chunk;
    }

    @Override
    public ArrowVarBinaryData getBinarySupplData() {
        return m_binarySuppl;
    }

    @Override
    public boolean isMissing(final int index) {
        return m_chunk.isMissing(index);
    }

    @Override
    public int getMaxCapacity() {
        return m_chunk.getMaxCapacity();
    }

    @Override
    public int getNumValues() {
        return m_chunk.getNumValues();
    }

    @Override
    public void setNumValues(final int numValues) {
        // TODO only set if any value is set.
        if (m_binarySuppl.get().getLastSet() != 0 && m_vector instanceof CustomStructVector) {
            ((CustomStructVector)m_vector).putChild("BinarySuppl", m_binarySuppl.get());
        } else {
            m_binarySuppl.get().clear();
        }
        m_vector.setValueCount(numValues);
    }

    @Override
    public synchronized void release() {
        m_binarySuppl.release();
        m_chunk.release();
        if (m_ref.decrementAndGet() == 0) {
            m_vector.close();
            if (m_vectorCloser != null) {
                m_vectorCloser.close();
            }
        }
    }

    @Override
    public synchronized void retain() {
        m_binarySuppl.retain();
        m_chunk.retain();
        m_ref.incrementAndGet();
    }

    @Override
    public StructVector get() {
        return m_vector;
    }

    @Override
    public int sizeOf() {
        return (int)(m_binarySuppl.sizeOf() + m_chunk.sizeOf() + m_vector.getValidityBuffer().capacity());
    }

    private static final class CustomStructVector extends StructVector {

        public CustomStructVector(final String name, final BufferAllocator allocator) {
            super(name, allocator, new FieldType(false, ArrowType.Struct.INSTANCE, null, null), null);
        }

        @Override
        public void putChild(final String name, final FieldVector vector) {
            super.putChild(name, vector);
        }

    }
}
