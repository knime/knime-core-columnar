
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.DataType;
import org.knime.core.data.DataTypeConfig;

public abstract class AbstractConfigurableDataTypeMapper<T extends DataTypeConfig> //
	extends AbstractDataTypeMapper //
	implements ConfigurableDataTypeMapper<T>
{

	private final Class<T> m_configClass;

	public AbstractConfigurableDataTypeMapper(final DataType dataType, final int version, final Class<T> configClass) {
		super(dataType, version);
		m_configClass = configClass;
	}

	@Override
	public Class<T> getConfigClass() {
		return m_configClass;
	}
}
