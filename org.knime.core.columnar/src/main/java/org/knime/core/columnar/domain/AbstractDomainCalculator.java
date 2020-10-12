
package org.knime.core.columnar.domain;

import org.knime.core.columnar.data.ColumnReadData;

abstract class AbstractDomainCalculator<C extends ColumnReadData, D extends Domain> implements DomainCalculator<C, D> {

	private final DomainMerger<D> m_merger;

	public AbstractDomainCalculator(final DomainMerger<D> merger) {
		m_merger = merger;
	}

	@Override
	public D createInitialDomain() {
		return m_merger.createInitialDomain();
	}

	@Override
	public D mergeDomains(final D original, final D additional) {
		return m_merger.mergeDomains(original, additional);
	}
}
