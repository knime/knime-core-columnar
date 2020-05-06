package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.Float8Vector;
import org.knime.core.columnar.data.DoubleData;

public class ArrowDoubleData extends AbstractFieldVectorData<Float8Vector> implements DoubleData {

	public ArrowDoubleData(BufferAllocator allocator) {
		super(allocator);
	}

	public ArrowDoubleData(Float8Vector vector) {
		super(vector);
	}

	@Override
	protected Float8Vector create(BufferAllocator allocator) {
		final Float8Vector vector = new Float8Vector("Float8Vector", allocator);
		return vector;
	}

	@Override
	public double getDouble(int index) {
		return m_vector.get(index);
	}

	@Override
	public void setDouble(int index, double value) {
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

}
