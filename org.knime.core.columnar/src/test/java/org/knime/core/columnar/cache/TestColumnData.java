package org.knime.core.columnar.cache;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStoreSchema;

class TestColumnData implements ColumnData {
	
	static ColumnStoreSchema createSchema(int numColumns) {
		return new ColumnStoreSchema() {
			@Override
			public int getNumColumns() {
				return numColumns;
			}
			
			@Override
			public ColumnDataSpec<?> getColumnDataSpec(int idx) {
				return null;
			}
		};
	}
	
	private final int m_sizeOf;
	
	private int m_refs = 0;
	
	TestColumnData(final int sizeOf) {
		m_sizeOf = sizeOf;
	}
	
	@Override
	public void ensureCapacity(int capacity) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int getMaxCapacity() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNumValues(int numValues) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getNumValues() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void release() {
		m_refs--;
	}

	@Override
	public void retain() {
		m_refs++;
	}

	@Override
	public int sizeOf() {
		return m_sizeOf;
	}
	
	int getRefs() {
		return m_refs;
	}
	
}
