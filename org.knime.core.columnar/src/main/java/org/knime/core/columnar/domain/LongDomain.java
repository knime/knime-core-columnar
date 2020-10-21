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
    private static final LongDomain EMPTY = new LongDomain(Long.MAX_VALUE, Long.MIN_VALUE);

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

    /**
     * Calculates the domain of {@link LongReadData}
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     */
    public static final class LongDomainCalculator implements DomainCalculator<LongReadData, LongDomain> {

        private long m_lower;

        private long m_upper;

        /**
         * Create calculator without initialization.
         */
        public LongDomainCalculator() {
            this(EMPTY);
        }

        /**
         * Create calculator with initialization.
         *
         * @param initialDomain the initial domain
         */
        public LongDomainCalculator(final LongDomain initialDomain) {
            m_lower = initialDomain.getLowerBound();
            m_upper = initialDomain.getUpperBound();
        }

        @Override
        public void update(final LongReadData data) {
            for (int i = 0; i < data.length(); i++) {
                if (!data.isMissing(i)) {
                    final long curr = data.getLong(i);
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
        public LongDomain getDomain() {
            return new LongDomain(m_lower, m_upper);
        }
    }

}