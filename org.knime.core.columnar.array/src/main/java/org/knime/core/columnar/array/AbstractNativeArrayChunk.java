package org.knime.core.columnar.array;

abstract class AbstractNativeArrayChunk<A> extends AbstractArrayChunk<A> {
	private long[] m_isMissing;

	protected AbstractNativeArrayChunk(A array, int capacity) {
		super(array, capacity);
		m_isMissing = new long[((int) capacity / 64) + 1];
	}

	@Override
	public boolean isMissing(int index) {
		// NB: inspired by imglib2
		return 1 == ((m_isMissing[((int) index >>> 6)] >>> ((index & 63))) & 1l);
	}

	@Override
	public void setMissing(int index) {
		// NB: inspired by imglib2
		final int i1 = (int) index >>> 6;
		m_isMissing[i1] = m_isMissing[i1] | 1l << (index & 63);
	}
}