package org.knime.core.columnar.table;

import org.knime.core.columnar.ColumnData;

// TODO type on ReadAcces / WriteAccess?
public interface ColumnDataAccess<C extends ColumnData> extends ReadValue, WriteValue {

	/**
	 * Updates underlying data.
	 * 
	 * @param obj
	 */
	void load(C obj);

	/**
	 * Fwd the internal cursor
	 */
	void fwd();

	/**
	 * resets internal cursor to -1
	 */
	void reset();
}
