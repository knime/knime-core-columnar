
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.columnar.domain.ColumnarDomain;

public interface DomainMapper<T extends ColumnarDomain> {

	Class<T> getColumnarDomainType();

	T mapToColumnarDomain(DataColumnDomain domain);

	DataColumnDomain mapFromColumnarDomain(T domain);
}
