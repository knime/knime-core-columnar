
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.columnar.domain.StringDomain;
import org.knime.core.data.columnar.types.StringColumnType;
import org.knime.core.data.def.StringCell;

public final class StringTypeMapper extends AbstractConfigurableDataTypeMapper<StringTypeConfig> //
		implements DomainDataTypeMapper<StringDomain> {

	public StringTypeMapper() {
		super(StringCell.TYPE, 0, StringTypeConfig.class);
	}

	@Override
	public StringColumnType createColumnType() {
		return createColumnType(new StringTypeConfig(false));
	}

	@Override
	public StringColumnType createColumnType(final StringTypeConfig config) {
		return new StringColumnType(config.isDictionaryEncoded());
	}

	@Override
	public Class<StringDomain> getDomainClass() {
		return StringDomain.class;
	}

	@Override
	public StringDomainMapper getDomainMapper() {
		return new StringDomainMapper();
	}
}
