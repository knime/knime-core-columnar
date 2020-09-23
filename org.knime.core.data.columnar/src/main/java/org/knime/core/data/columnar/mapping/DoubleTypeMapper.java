
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.columnar.domain.DoubleDomain;
import org.knime.core.data.columnar.types.DoubleColumnType;
import org.knime.core.data.def.DoubleCell;

public final class DoubleTypeMapper extends AbstractDataTypeMapper implements DomainDataTypeMapper<DoubleDomain> {

	public DoubleTypeMapper() {
		super(DoubleCell.TYPE, 0);
	}

	@Override
	public DoubleColumnType createColumnType() {
		return DoubleColumnType.INSTANCE;
	}

	@Override
	public Class<DoubleDomain> getDomainClass() {
		return DoubleDomain.class;
	}

	@Override
	public DoubleDomainMapper getDomainMapper() {
		return new DoubleDomainMapper();
	}
}
