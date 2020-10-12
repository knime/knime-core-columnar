
package org.knime.core.columnar.domain;

import org.knime.core.columnar.data.IntData.IntReadData;

/**
 * Domain for int values.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class IntDomain implements BoundedDomain<Integer> {

    /** Empty int domain **/
    public static final IntDomain EMPTY = new IntDomain(Integer.MAX_VALUE, Integer.MIN_VALUE);

    private final int m_lower;

    private final int m_upper;

    /**
     * Create a new domain with lower and upper bound initialized.
     *
     * @param lower bound
     * @param upper bound
     */
    public IntDomain(final int lower, final int upper) {
        m_lower = lower;
        m_upper = upper;
    }

    @Override
    public boolean hasLowerBound() {
        return m_lower <= m_upper;
    }

    @Override
    public Integer getLowerBound() {
        return hasLowerBound() ? m_lower : null;
    }

    @Override
    public boolean hasUpperBound() {
        return m_upper >= m_lower;
    }

    @Override
    public Integer getUpperBound() {
        return hasUpperBound() ? m_upper : null;
    }

    private static final class IntDomainMerger extends AbstractDomainMerger<IntDomain> {

        public IntDomainMerger(final IntDomain initialDomain) {
            super(initialDomain != null ? initialDomain : IntDomain.EMPTY);
        }

        @Override
        public IntDomain mergeDomains(final IntDomain original, final IntDomain additional) {
            return new IntDomain( //
                Math.min(original.m_lower, additional.m_lower), //
                Math.max(original.m_upper, additional.m_upper));
        }
    }

    /**
     * Calculates the domain of {@link IntReadData}
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     */
    public static final class IntDomainCalculator extends AbstractDomainCalculator<IntReadData, IntDomain> {

        /**
         * Create calculator without initialization.
         */
        public IntDomainCalculator() {
            this(null);
        }

        /**
         * Create calculator with initialization.
         *
         * @param initialDomain the initial domain
         */
        public IntDomainCalculator(final IntDomain initialDomain) {
            super(new IntDomainMerger(initialDomain));
        }

        @Override
        public IntDomain calculateDomain(final IntReadData data) {
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
    }
}
