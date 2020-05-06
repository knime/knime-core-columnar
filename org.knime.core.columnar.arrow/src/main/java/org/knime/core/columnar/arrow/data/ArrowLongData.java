package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.knime.core.columnar.data.LongData;

public class ArrowLongData extends AbstractFieldVectorData<BigIntVector> implements LongData {

	public ArrowLongData(BufferAllocator allocator) {
		super(allocator);
	}

	public ArrowLongData(BigIntVector vector) {
		super(vector);
	}

	@Override
	protected BigIntVector create(BufferAllocator allocator) {
		return new BigIntVector("IntVector", allocator);
	}

	@Override
	public long getLong(int index) {
		return m_vector.get(index);
	}

	@Override
	public void setLong(int index, long value) {
		m_vector.set(index, value);
	}

	@Override
	public void ensureCapacityInternal(int chunkSize) {
		m_vector.allocateNew(chunkSize);
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

}
