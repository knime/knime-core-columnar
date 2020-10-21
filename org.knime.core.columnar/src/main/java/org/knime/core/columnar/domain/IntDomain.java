
package org.knime.core.columnar.domain;

import org.knime.core.columnar.data.IntData.IntReadData;

/**
 * Domain for int values.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class IntDomain implements BoundedDomain<Integer> {

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

    /**
     * Calculates the domain of {@link IntReadData}
     *
     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
     */
    public static final class IntDomainCalculator implements DomainCalculator<IntReadData, IntDomain> {

        private int m_lower = Integer.MAX_VALUE;

        private int m_upper = Integer.MIN_VALUE;

        /**
         * Create calculator without initialization.
         */
        public IntDomainCalculator() {
        }

        /**
         * Create calculator with initialization.
         *
         * @param initialDomain the initial domain
         */
        public IntDomainCalculator(final IntDomain initialDomain) {
            if (initialDomain.isValid()) {
                m_lower = initialDomain.getLowerBound();
                m_upper = initialDomain.getUpperBound();
            }
        }

        @Override
        public void update(final IntReadData data) {
            for (int i = 0; i < data.length(); i++) {
                if (!data.isMissing(i)) {
                    final int curr = data.getInt(i);
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
        public IntDomain getDomain() {
            return new IntDomain(m_lower, m_upper);
        }
    }
}
