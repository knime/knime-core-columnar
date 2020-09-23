
package org.knime.core.data.columnar.domain;

import org.knime.core.columnar.data.IntData.IntReadData;

public final class IntDomain implements NumericDomain<Integer> {

	private static final IntDomain EMPTY = new IntDomain(Integer.MAX_VALUE, Integer.MIN_VALUE);

	private final int m_lower;

	private final int m_upper;

	public IntDomain(final int lower, final int upper) {
		m_lower = lower;
		m_upper = upper;
	}

	@Override
	public Integer getLowerBound() {
		return m_lower > m_upper ? null : m_lower;
	}

	@Override
	public Integer getUpperBound() {
		return m_upper < m_lower ? null : m_upper;
	}

	public static final class IntDomainCalculator extends AbstractDomainCalculator<IntReadData, IntDomain> {

		public IntDomainCalculator() {
			this(EMPTY);
		}

		public IntDomainCalculator(final IntDomain initialDomain) {
			super(initialDomain);
		}

		@Override
		public IntDomain apply(final IntReadData data) {
			int lower = Integer.MAX_VALUE;
			int upper = Integer.MIN_VALUE;
			for (int i = 0; i < data.length(); i++) {
				if (!data.isMissing(i)) {
					final int curr = data.getInt(i);
					if (curr < lower) {
						lower = curr;
					}
					if (curr > upper) {
						upper = curr;
					}
				}
			}
			return new IntDomain(lower, upper);
		}

		@Override
		public IntDomain merge(final IntDomain original, final IntDomain additional) {
			return new IntDomain( //
				Math.min(original.m_lower, additional.m_lower), //
				Math.max(original.m_upper, additional.m_upper));
		}
	}
}
