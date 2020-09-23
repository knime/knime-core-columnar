
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.columnar.domain.IntDomain;
import org.knime.core.data.columnar.types.IntColumnType;
import org.knime.core.data.def.IntCell;

public final class IntTypeMapper extends AbstractDataTypeMapper implements DomainDataTypeMapper<IntDomain> {

	public IntTypeMapper() {
		super(IntCell.TYPE, 0);
	}

	@Override
	public IntColumnType createColumnType() {
		return IntColumnType.INSTANCE;
	}

	@Override
	public Class<IntDomain> getDomainClass() {
		return IntDomain.class;
	}

	@Override
	public IntDomainMapper getDomainMapper() {
		return new IntDomainMapper();
	}
}
