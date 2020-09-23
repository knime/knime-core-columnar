
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.IntValue;
import org.knime.core.data.columnar.domain.IntDomain;
import org.knime.core.data.def.IntCell.IntCellFactory;

public final class IntDomainMapper extends AbstractNumericDomainMapper<IntValue, Integer, IntDomain> {

	public IntDomainMapper() {
		super(IntDomain.class, IntValue::getIntValue, IntDomain::new, IntCellFactory::create);
	}
}
