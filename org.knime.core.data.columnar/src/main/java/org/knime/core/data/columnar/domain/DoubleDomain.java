
package org.knime.core.data.columnar.domain;

import org.knime.core.columnar.data.DoubleData.DoubleReadData;

public final class DoubleDomain implements NumericDomain<Double> {

	private static final DoubleDomain EMPTY = new DoubleDomain(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

	private final double m_lower;

	private final double m_upper;

	public DoubleDomain(final double lower, final double upper) {
		m_lower = lower;
		m_upper = upper;
	}

	@Override
	public Double getLowerBound() {
		return m_lower == Double.POSITIVE_INFINITY && m_upper != Double.POSITIVE_INFINITY ? null : m_lower;
	}

	@Override
	public Double getUpperBound() {
		return m_upper == Double.NEGATIVE_INFINITY && m_lower != Double.NEGATIVE_INFINITY ? null : m_upper;
	}

	public static final class DoubleDomainCalculator extends AbstractDomainCalculator<DoubleReadData, DoubleDomain> {

		public DoubleDomainCalculator() {
			this(EMPTY);
		}

		public DoubleDomainCalculator(final DoubleDomain initialDomain) {
			super(initialDomain);
		}

		@Override
		public DoubleDomain apply(final DoubleReadData data) {
			double lower = Double.POSITIVE_INFINITY;
			double upper = Double.NEGATIVE_INFINITY;
			for (int i = 0; i < data.length(); i++) {
				if (!data.isMissing(i)) {
					final double curr = data.getDouble(i);
					if (curr < lower) {
						lower = curr;
					}
					if (curr > upper) {
						upper = curr;
					}
				}
			}
			return new DoubleDomain(lower, upper);
		}

		@Override
		public DoubleDomain merge(final DoubleDomain original, final DoubleDomain additional) {
			return new DoubleDomain( //
					Math.min(original.m_lower, additional.m_lower), //
					Math.max(original.m_upper, additional.m_upper));
		}
	}
}
