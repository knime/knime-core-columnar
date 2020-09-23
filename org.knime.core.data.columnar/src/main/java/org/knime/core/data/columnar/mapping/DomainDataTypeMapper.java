
package org.knime.core.data.columnar.mapping;

import org.knime.core.data.columnar.domain.ColumnarDomain;

public interface DomainDataTypeMapper<T extends ColumnarDomain> extends DataTypeMapper {

	Class<T> getDomainClass();

	DomainMapper<? super T> getDomainMapper();
}
