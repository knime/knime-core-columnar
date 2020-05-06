package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.VarBinaryVector;
import org.knime.core.columnar.data.VarBinaryData;

public class ArrowVarBinaryData extends AbstractFieldVectorData<VarBinaryVector> implements VarBinaryData {

	public ArrowVarBinaryData(BufferAllocator allocator) {
		super(allocator);
	}

	public ArrowVarBinaryData(VarBinaryVector vector) {
		super(vector);
	}

	@Override
	protected VarBinaryVector create(BufferAllocator allocator) {
		return new VarBinaryVector("BinaryVector", allocator);
	}

	@Override
	public byte[] getBytes(int index) {
		return m_vector.get(index);
	}

	@Override
	public void setBytes(int index, byte[] value) {
		m_vector.setSafe(index, value);
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
