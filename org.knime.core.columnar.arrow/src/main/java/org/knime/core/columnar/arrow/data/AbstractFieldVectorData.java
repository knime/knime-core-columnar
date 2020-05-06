package org.knime.core.columnar.arrow.data;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

abstract class AbstractFieldVectorData<F extends FieldVector> implements ArrowData<F> {

	protected final F m_vector;

	private final AtomicInteger m_refCounter = new AtomicInteger(1);

	private int m_maxCapacity;

	AbstractFieldVectorData(BufferAllocator allocator) {
		m_vector = create(allocator);
	}

	protected abstract F create(BufferAllocator allocator);

	AbstractFieldVectorData(F vector) {
		m_vector = vector;
	}

	@Override
	public F get() {
		return m_vector;
	}

	@Override
	public boolean isMissing(int index) {
		return m_vector.isNull(index);
	}

	@Override
	public int getMaxCapacity() {
		return m_maxCapacity;
	}

	@Override
	public void ensureCapacity(int chunkSize) {
		if (chunkSize > m_maxCapacity) {
			ensureCapacityInternal(chunkSize);
			m_maxCapacity = chunkSize;
		}
	}

	abstract void ensureCapacityInternal(int chunkSize);

	@Override
	public int getNumValues() {
		return m_vector.getValueCount();
	}

	@Override
	public void setNumValues(int numValues) {
		m_vector.setValueCount(numValues);
	}

	// TODO thread safety for ref-counting
	@Override
	public synchronized void release() {
		if (m_refCounter.decrementAndGet() == 0) {
			m_vector.close();
		}
	}

	@Override
	public synchronized void retain() {
		m_refCounter.getAndIncrement();
	}

	@Override
	public int sizeOf() {
		return (int) (m_vector.getDataBuffer().capacity() + m_vector.getValidityBuffer().capacity());
	}

	@Override
	public String toString() {
		return m_vector.toString();
	}

}
