
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.StringValue;
import org.knime.core.data.columnar.domain.StringDomain;
import org.knime.core.data.def.StringCell.StringCellFactory;

public class StringDomainMapper extends AbstractNominalDomainMapper<StringValue, String, StringDomain> {

	public StringDomainMapper() {
		super(StringDomain.class, StringValue::getStringValue, StringDomain::new, StringCellFactory::create);
	}
}
