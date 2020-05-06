package org.knime.core.columnar.array;

import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractArrayChunk<A> implements ArrayData {

	private final int m_capacity;
	private final AtomicInteger m_ref = new AtomicInteger(1);

	private int m_numValues;

	protected final A m_array;

	AbstractArrayChunk(A array, int capacity) {
		m_array = array;
		m_capacity = capacity;
	}

	@Override
	public void release() {
		if (m_ref.getAndDecrement() == 0) {
			// NB: Nothing? array / missing should be collected by GC anyways?
		}
	}

	@Override
	public int getMaxCapacity() {
		return m_capacity;
	}

	@Override
	public int getNumValues() {
		return m_numValues;
	}

	@Override
	public void setNumValues(int numValues) {
		m_numValues = numValues;
	}

	@Override
	public void retain() {
		m_ref.getAndIncrement();
	}
}
