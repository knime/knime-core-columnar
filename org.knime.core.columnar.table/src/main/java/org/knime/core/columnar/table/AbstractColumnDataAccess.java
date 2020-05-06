package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnData;

public abstract class AbstractColumnDataAccess<C extends ColumnData> implements ColumnDataAccess<C> {

	protected int m_index;
	protected C m_data;

	@Override
	public void load(C data) {
		m_data = data;
	}

	@Override
	public void fwd() {
		m_index++;
	}

	@Override
	public void reset() {
		m_index = -1;
	}
}
