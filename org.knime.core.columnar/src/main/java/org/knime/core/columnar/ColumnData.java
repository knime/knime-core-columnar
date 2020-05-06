package org.knime.core.columnar;

public interface ColumnData {

	// WRITE
	/**
	 * Set the maximum capacity of values
	 */
	void ensureCapacity(int capacity);

	/**
	 * @return maximum capacity of an array
	 */
	int getMaxCapacity();

	/**
	 * TODO rename to 'finishWriting'?
	 * 
	 * @param numValues set number of logically written values
	 */
	void setNumValues(int numValues);

	// READ
	/**
	 * @return number of values set
	 */
	int getNumValues();

	// REFERENCED
	/**
	 * Release reference
	 */
	void release();

	/**
	 * Retain reference
	 */
	void retain();
}
