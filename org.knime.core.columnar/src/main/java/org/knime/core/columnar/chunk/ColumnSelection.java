package org.knime.core.columnar.chunk;

// TODO we could also provide pre-loading hints here?
public interface ColumnSelection {
	/**
	 * TODO implement as ranges, e.g. return a ColumnIndicesSelection with a method
	 * called 'contains(int i)'?
	 * 
	 * @return the selected column indices in ascending order
	 */
	int[] get();
}
