package org.knime.core.columnar.array;

import org.knime.core.columnar.data.DoubleData;

class DoubleArrayChunk extends AbstractNativeArrayChunk<double[]> implements DoubleData {

	// Read case
	public DoubleArrayChunk(int capacity) {
		super(new double[capacity], capacity);
	}

	@Override
	public void ensureCapacity(int capacity) {
		// Nothing?
	}

	@Override
	public double getDouble(int index) {
		return m_array[index];
	}

	@Override
	public void setDouble(int index, double val) {
		m_array[index] = val;
	}
}
