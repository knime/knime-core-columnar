
package org.knime.core.data.columnar.domain;

import org.knime.core.columnar.data.ColumnData;

public abstract class AbstractDomainCalculator<C extends ColumnData, D extends ColumnarDomain> //
	implements DomainCalculator<C, D>
{

	private final D m_initialDomain;

	public AbstractDomainCalculator(final D initialDomain) {
		m_initialDomain = initialDomain;
	}

	@Override
	public D createInitialDomain() {
		return m_initialDomain;
	}
}
