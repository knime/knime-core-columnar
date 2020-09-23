
package org.knime.core.data.columnar.domain;

import java.util.Set;

public interface NominalDomain<T> extends ColumnarDomain {

	Set<T> getValues();
}
