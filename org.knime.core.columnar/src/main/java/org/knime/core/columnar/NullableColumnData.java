package org.knime.core.columnar;

public interface NullableColumnData extends ColumnData {
	/**
	 * @param set value missing at index. Default is false.
	 */
	void setMissing(int index);

	/**
	 * @param index of value
	 * @return true, if value is missing. Default is false.
	 */
	boolean isMissing(int index);
}
