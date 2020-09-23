
package org.knime.core.data.columnar.domain;

import org.knime.core.columnar.data.LongData.LongReadData;

public final class LongDomain implements NumericDomain<Long> {

	private static final LongDomain EMPTY = new LongDomain(Long.MAX_VALUE, Long.MIN_VALUE);

	private final long m_lower;

	private final long m_upper;

	public LongDomain(final long lower, final long upper) {
		m_lower = lower;
		m_upper = upper;
	}

	@Override
	public Long getLowerBound() {
		return m_lower > m_upper ? null : m_lower;
	}

	@Override
	public Long getUpperBound() {
		return m_upper < m_lower ? null : m_upper;
	}

	public static final class LongDomainCalculator extends AbstractDomainCalculator<LongReadData, LongDomain> {

		public LongDomainCalculator() {
			this(EMPTY);
		}

		public LongDomainCalculator(final LongDomain initialDomain) {
			super(initialDomain);
		}

		@Override
		public LongDomain apply(final LongReadData data) {
			long lower = Long.MAX_VALUE;
			long upper = Long.MIN_VALUE;
			for (int i = 0; i < data.length(); i++) {
				if (!data.isMissing(i)) {
					final long curr = data.getLong(i);
					if (curr < lower) {
						lower = curr;
					}
					if (curr > upper) {
						upper = curr;
					}
				}
			}
			return new LongDomain(lower, upper);
		}

		@Override
		public LongDomain merge(final LongDomain original, final LongDomain additional) {
			return new LongDomain( //
					Math.min(original.m_lower, additional.m_lower), //
					Math.max(original.m_upper, additional.m_upper));
		}
	}
}
