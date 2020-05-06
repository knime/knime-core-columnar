package org.knime.core.columnar.table;

/**
 * Base interface for value through which data values are read.
 */
public interface NullableReadValue extends ReadValue {

	boolean isMissing();
}
