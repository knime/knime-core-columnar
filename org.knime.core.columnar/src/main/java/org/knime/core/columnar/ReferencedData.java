package org.knime.core.columnar;

public interface ReferencedData {

	/**
	 * Release reference
	 */
	void release();

	/**
	 * Retain reference
	 */
	void retain();

	/**
	 * Determine the size of the chunk in bytes or a pessimistic estimate thereof.
	 * 
	 * @return size of chunk in bytes
	 */
	int sizeOf();

}
