package org.knime.core.columnar.table;

/**
 * Base interface for proxies through which data values are written.
 */
public interface NullableWriteValue extends WriteValue {

	// TODO what is the default?
	void setMissing();
}
