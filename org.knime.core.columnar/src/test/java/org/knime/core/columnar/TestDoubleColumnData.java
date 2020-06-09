package org.knime.core.columnar;

import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.data.DoubleData;

class TestDoubleColumnData implements ColumnData, DoubleData {

	public Double[] m_values;
	public int m_numValues;

	private int m_chunkCapacity;

	private AtomicInteger m_ref = new AtomicInteger(1);

	TestDoubleColumnData(int chunkCapacity) {
		m_chunkCapacity = chunkCapacity;
	}

	public TestDoubleColumnData(Double[] doubles) {
		m_values = doubles;
		m_numValues = doubles.length;
	}

	@Override
	public void release() {
		if (m_ref.decrementAndGet() == 0) {
			m_values = null;
		}
	}

	@Override
	public void retain() {
		m_ref.incrementAndGet();
	}

	@Override
	public int sizeOf() {
		return m_values.length * 8;
	}

	@Override
	public void ensureCapacity(int capacity) {
		m_values = new Double[m_chunkCapacity];
	}

	@Override
	public int getMaxCapacity() {
		return m_chunkCapacity;
	}

	@Override
	public void setNumValues(int numValues) {
		m_numValues = numValues;
	}

	@Override
	public int getNumValues() {
		return m_numValues;
	}

	@Override
	public void setMissing(int index) {
		m_values[index] = null;
	}

	@Override
	public boolean isMissing(int index) {
		return m_values[index] == null;
	}

	@Override
	public double getDouble(int index) {
		return m_values[index];
	}

	@Override
	public void setDouble(int index, double val) {
		m_values[index] = val;
	}

	public Double[] get() {
		return m_values;
	}

}
