
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.DataType;

public abstract class AbstractDataTypeMapper implements DataTypeMapper {

	private final DataType m_dataType;

	private final int m_version;

	public AbstractDataTypeMapper(final DataType dataType, final int version) {
		m_dataType = dataType;
		m_version = version;
	}

	@Override
	public DataType getDataType() {
		return m_dataType;
	}

	@Override
	public int getVersion() {
		return m_version;
	}
}
