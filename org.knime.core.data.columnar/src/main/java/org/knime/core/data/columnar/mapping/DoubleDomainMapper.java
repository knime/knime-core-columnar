
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.DoubleValue;
import org.knime.core.data.columnar.domain.DoubleDomain;
import org.knime.core.data.def.DoubleCell.DoubleCellFactory;

public final class DoubleDomainMapper extends AbstractNumericDomainMapper<DoubleValue, Double, DoubleDomain> {

	public DoubleDomainMapper() {
		super(DoubleDomain.class, DoubleValue::getDoubleValue, DoubleDomain::new, DoubleCellFactory::create);
	}
}
