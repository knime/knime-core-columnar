
package org.knime.core.columnar.domain;

import org.knime.core.columnar.data.DoubleData.DoubleReadData;

/**
 * Domain for double values.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class DoubleDomain implements BoundedDomain<Double> {

    private static final DoubleDomain EMPTY = new DoubleDomain(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

    private final double m_lower;

    private final double m_upper;

    /**
     * Create a new domain with lower and upper bound initialized.
     *
     * @param lower bound
     * @param upper bound
     */
    public DoubleDomain(final double lower, final double upper) {
        m_lower = lower;
        m_upper = upper;
    }

    @Override
    public boolean hasLowerBound() {
        return m_lower != Double.POSITIVE_INFINITY || m_upper == Double.POSITIVE_INFINITY;
    }

    @Override
    public Double getLowerBound() {
        return hasLowerBound() ? m_lower : null;
    }

    @Override
    public boolean hasUpperBound() {
        return m_upper != Double.NEGATIVE_INFINITY || m_lower == Double.NEGATIVE_INFINITY;
    }

    @Override
    public Double getUpperBound() {
        return hasUpperBound() ? m_upper : null;
    }

    /**
     * Calculates the domain of {@link DoubleReadData}
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     */
    public static final class DoubleDomainCalculator implements DomainCalculator<DoubleReadData, DoubleDomain> {

        private double m_lower;

        private double m_upper;

        /**
         * Create calculator without initialization.
         */
        public DoubleDomainCalculator() {
            this(EMPTY);
        }

        /**
         * Create calculator with initialization.
         *
         * @param initialDomain the initial domain
         */
        public DoubleDomainCalculator(final DoubleDomain initialDomain) {
            m_lower = initialDomain.getLowerBound();
            m_upper = initialDomain.getUpperBound();
        }

        @Override
        public void update(final DoubleReadData data) {
            for (int i = 0; i < data.length(); i++) {
                if (!data.isMissing(i)) {
                    final double curr = data.getDouble(i);
                    if (curr < m_lower) {
                        m_lower = curr;
                    }
                    if (curr > m_upper) {
                        m_upper = curr;
                    }
                }
            }
        }

        @Override
        public DoubleDomain getDomain() {
            return new DoubleDomain(m_lower, m_upper);
        }
    }
}
