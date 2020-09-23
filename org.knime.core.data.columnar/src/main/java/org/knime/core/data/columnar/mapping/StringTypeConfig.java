
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.DataTypeConfig;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;

/**
 * Configuration for columns of type {@link StringCell#TYPE}.
 */
public final class StringTypeConfig implements DataTypeConfig {

	private final boolean m_isDictEncoded;

	public StringTypeConfig(final boolean isDictEncoded) {
		m_isDictEncoded = isDictEncoded;
	}

	public boolean isDictionaryEncoded() {
		return m_isDictEncoded;
	}

	public static final class StringTypeConfigSerializer implements DataTypeConfigSerializer<StringTypeConfig> {

		private static final String CFG_KEY_IS_DICT_ENCODED = "is_dict_encoded";

		@Override
		public void save(final StringTypeConfig metaData, final ConfigWO config) {
			config.addBoolean(CFG_KEY_IS_DICT_ENCODED, metaData.isDictionaryEncoded());
		}

		@Override
		public StringTypeConfig load(final ConfigRO config) throws InvalidSettingsException {
			return new StringTypeConfig(config.getBoolean(CFG_KEY_IS_DICT_ENCODED));
		}
	}
}
