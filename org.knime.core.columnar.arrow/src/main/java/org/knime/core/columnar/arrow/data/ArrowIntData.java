package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.knime.core.columnar.data.IntData;

public class ArrowIntData extends AbstractFieldVectorData<IntVector> implements IntData {

	public ArrowIntData(BufferAllocator allocator) {
		super(allocator);
	}

	public ArrowIntData(IntVector vector) {
		super(vector);
	}

	@Override
	protected IntVector create(BufferAllocator allocator) {
		return new IntVector("IntVector", allocator);
	}

	@Override
	public int getInt(int index) {
		return m_vector.get(index);
	}

	@Override
	public void setInt(int index, int value) {
		m_vector.set(index, value);
	}

	@Override
	public void setMissing(int index) {
		// TODO we can speed things likely up directly accessing validity buffer
		BitVectorHelper.unsetBit(m_vector.getValidityBuffer(), index);
	}

	@Override
	public double getDouble(int index) {
		return m_vector.get(index);
	}

	@Override
	public void ensureCapacityInternal(int chunkSize) {
		m_vector.allocateNew(chunkSize);
	}

}
