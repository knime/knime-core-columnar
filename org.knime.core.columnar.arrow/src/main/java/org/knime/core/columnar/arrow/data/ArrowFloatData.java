package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.Float4Vector;
import org.knime.core.columnar.data.FloatData;

public class ArrowFloatData extends AbstractFieldVectorData<Float4Vector> implements FloatData {

	public ArrowFloatData(BufferAllocator allocator) {
		super(allocator);
	}

	public ArrowFloatData(Float4Vector vector) {
		super(vector);
	}

	@Override
	protected Float4Vector create(BufferAllocator allocator) {
		return new Float4Vector("Float4Vector", allocator);
	}

	@Override
	public float getFloat(int index) {
		return m_vector.get(index);
	}

	@Override
	public void setFloat(int index, float value) {
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
