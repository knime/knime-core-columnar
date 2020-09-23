
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.LongValue;
import org.knime.core.data.columnar.domain.LongDomain;
import org.knime.core.data.def.LongCell.LongCellFactory;

public final class LongDomainMapper extends AbstractNumericDomainMapper<LongValue, Long, LongDomain> {

	public LongDomainMapper() {
		super(LongDomain.class, LongValue::getLongValue, LongDomain::new, LongCellFactory::create);
	}
}
