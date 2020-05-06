package org.knime.core.columnar.array;

import org.knime.core.columnar.data.StringData;

// TODO we may want to store sparse arrays etc differently.
class StringArrayChunk extends AbstractArrayChunk<String[]> implements StringData {

	// Read case
	public StringArrayChunk(int capacity) {
		super(new String[capacity], capacity);
	}

	@Override
	public String getString(int index) {
		return m_array[index];
	}

	@Override
	public void setString(int index, String val) {
		m_array[index] = val;
	}

	@Override
	public void setMissing(int index) {
		m_array[index] = null;
	}

	@Override
	public boolean isMissing(int index) {
		return m_array[index] == null;
	}

	@Override
	public void ensureCapacity(int capacity) {
		// TODO Auto-generated method stub

	}
}
