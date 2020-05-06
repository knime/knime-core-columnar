package org.knime.core.columnar.arrow.data;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.knime.core.columnar.data.BinarySupplData;

public class ArrowBinarySupplementData<C extends ArrowData<?>> implements BinarySupplData<C>, ArrowData<StructVector> {

	private AtomicInteger m_ref = new AtomicInteger(1);
	private final ArrowVarBinaryData m_binarySuppl;
	private C m_chunk;

	private final StructVector m_vector;

	public ArrowBinarySupplementData(BufferAllocator allocator, C chunk) {
		m_chunk = chunk;
		m_binarySuppl = new ArrowVarBinaryData(allocator);
		final CustomStructVector vector = new CustomStructVector("BinarySuppl", allocator);

		vector.putChild("Data", m_chunk.get());

		m_vector = vector;
	}

	public ArrowBinarySupplementData(StructVector vector, C chunk) {
		m_vector = vector;
		m_chunk = chunk;
		m_binarySuppl = new ArrowVarBinaryData((VarBinaryVector) vector.getChildByOrdinal(1));
	}

	@Override
	public void setMissing(int index) {
		m_chunk.setMissing(index);
	}

	@Override
	public void ensureCapacity(int chunkSize) {
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
	public boolean isMissing(int index) {
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
	public void setNumValues(int numValues) {
		// TODO only set if any value is set.
		if (m_binarySuppl.get().getLastSet() != 0 && m_vector instanceof CustomStructVector) {
			((CustomStructVector) m_vector).putChild("BinarySuppl", m_binarySuppl.get());
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
		return (int) (m_binarySuppl.sizeOf() + m_chunk.sizeOf() + m_vector.getValidityBuffer().capacity());
	}

	private static final class CustomStructVector extends StructVector {

		public CustomStructVector(String name, BufferAllocator allocator) {
			super(name, allocator, null, null);
		}

		@Override
		public void putChild(String name, FieldVector vector) {
			super.putChild(name, vector);
		}

	}
}
