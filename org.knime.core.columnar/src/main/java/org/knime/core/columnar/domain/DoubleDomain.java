
package org.knime.core.columnar.domain;

import org.knime.core.columnar.data.DoubleData.DoubleReadData;

/**
 * Domain for double values.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class DoubleDomain implements BoundedDomain<Double> {

    /** Empty double domain **/
    public static final DoubleDomain EMPTY = new DoubleDomain(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

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
    public static final class DoubleDomainCalculator extends AbstractDomainCalculator<DoubleReadData, DoubleDomain> {

        /**
         * Create calculator without initialization.
         */
        public DoubleDomainCalculator() {
            this(null);
        }

        /**
         * Create calculator with initialization.
         *
         * @param initialDomain the initial domain
         */
        public DoubleDomainCalculator(final DoubleDomain initialDomain) {
            super(new DoubleDomainMerger(initialDomain));
        }

        @Override
        public DoubleDomain calculateDomain(final DoubleReadData data) {
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
    }

    private static final class DoubleDomainMerger extends AbstractDomainMerger<DoubleDomain> {

        public DoubleDomainMerger(final DoubleDomain initialDomain) {
            super(initialDomain != null ? initialDomain : DoubleDomain.EMPTY);
        }

        @Override
        public DoubleDomain mergeDomains(final DoubleDomain original, final DoubleDomain additional) {
            return new DoubleDomain( //
                Math.min(original.m_lower, additional.m_lower), //
                Math.max(original.m_upper, additional.m_upper));
        }
    }

}
