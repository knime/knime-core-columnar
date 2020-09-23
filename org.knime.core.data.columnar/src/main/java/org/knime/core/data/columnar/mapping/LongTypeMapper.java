
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.columnar.domain.LongDomain;
import org.knime.core.data.columnar.types.LongColumnType;
import org.knime.core.data.def.LongCell;

public final class LongTypeMapper extends AbstractDataTypeMapper implements DomainDataTypeMapper<LongDomain> {

	public LongTypeMapper() {
		super(LongCell.TYPE, 0);
	}

	@Override
	public LongColumnType createColumnType() {
		return LongColumnType.INSTANCE;
	}

	@Override
	public Class<LongDomain> getDomainClass() {
		return LongDomain.class;
	}

	@Override
	public LongDomainMapper getDomainMapper() {
		return new LongDomainMapper();
	}
}
