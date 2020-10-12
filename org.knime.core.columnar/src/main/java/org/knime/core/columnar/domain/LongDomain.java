package org.knime.core.columnar.domain;

import org.knime.core.columnar.data.LongData.LongReadData;

/**
 * Domain for long values.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class LongDomain implements BoundedDomain<Long> {

    /** Empty long domain **/
    public static final LongDomain EMPTY = new LongDomain(Long.MAX_VALUE, Long.MIN_VALUE);

    private final long m_lower;

    private final long m_upper;

    /**
     * Create a new domain with lower and upper bound initialized.
     *
     * @param lower bound
     * @param upper bound
     */
    public LongDomain(final long lower, final long upper) {
        m_lower = lower;
        m_upper = upper;
    }

    @Override
    public boolean hasLowerBound() {
        return m_lower <= m_upper;
    }

    @Override
    public Long getLowerBound() {
        return hasLowerBound() ? m_lower : null;
    }

    @Override
    public boolean hasUpperBound() {
        return m_upper >= m_lower;
    }

    @Override
    public Long getUpperBound() {
        return hasUpperBound() ? m_upper : null;
    }

    private static final class LongDomainMerger extends AbstractDomainMerger<LongDomain> {

        public LongDomainMerger(final LongDomain initialDomain) {
            super(initialDomain != null ? initialDomain : LongDomain.EMPTY);
        }

        @Override
        public LongDomain mergeDomains(final LongDomain original, final LongDomain additional) {
            return new LongDomain( //
                Math.min(original.m_lower, additional.m_lower), //
                Math.max(original.m_upper, additional.m_upper));
        }
    }

    /**
     * Calculates the domain of {@link LongReadData}
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     */
    public static final class LongDomainCalculator extends AbstractDomainCalculator<LongReadData, LongDomain> {

        /**
         * Create calculator without initialization.
         */
        public LongDomainCalculator() {
            this(null);
        }

        /**
         * Create calculator with initialization.
         *
         * @param initialDomain the initial domain
         */
        public LongDomainCalculator(final LongDomain initialDomain) {
            super(new LongDomainMerger(initialDomain));
        }

        @Override
        public LongDomain calculateDomain(final LongReadData data) {
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
    }

}