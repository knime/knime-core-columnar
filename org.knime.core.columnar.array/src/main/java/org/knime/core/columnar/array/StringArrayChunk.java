package org.knime.core.columnar.array;

import org.knime.core.columnar.data.StringData;

// TODO we may want to store sparse arrays etc differently.
class StringArrayChunk extends AbstractArrayChunk<String[]> implements StringData {
	
	private int m_stringLengthTotal = 0;

	private int m_sizeOfIndex = 0;

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
	
	@Override
	public int sizeOf() {
		for (; m_sizeOfIndex < getNumValues(); m_sizeOfIndex++) {
			m_stringLengthTotal += getString(m_sizeOfIndex).length();
		}
		// for a 64 bit VM with more than 32 GB heap space:
		// 24 bytes for String array object
		// 8 bytes per reference on String
		// 32 bytes per String object (including held character array)
		// 2 bytes per character
		return 24 + 8 * getMaxCapacity() + 32 * getNumValues() + m_stringLengthTotal * 2;
	}

}
