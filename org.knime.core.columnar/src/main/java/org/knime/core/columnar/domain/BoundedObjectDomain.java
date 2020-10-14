/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Oct 4, 2020 (dietzc): created
 */
package org.knime.core.columnar.domain;

import java.util.Comparator;

import org.knime.core.columnar.data.ObjectData.ObjectReadData;

/**
 * Bounded domain for T
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @param <T> type of objects
 */
public class BoundedObjectDomain<T> implements BoundedDomain<T> {

    private static final BoundedObjectDomain<?> EMPTY = new BoundedObjectDomain<>();

    private final T m_lower;

    private final T m_upper;

    /**
     * Create an empty bounded domain
     */
    public BoundedObjectDomain() {
        this(null, null);
    }

    /**
     * Create an initialized bounded domain with lower and upper.
     *
     * @param lower bound
     * @param upper bound
     */
    public BoundedObjectDomain(final T lower, final T upper) {
        m_lower = lower;
        m_upper = upper;
    }

    @Override
    public boolean hasLowerBound() {
        return m_lower != null;
    }

    @Override
    public T getLowerBound() {
        return m_lower;
    }

    @Override
    public boolean hasUpperBound() {
        return m_upper != null;
    }

    @Override
    public T getUpperBound() {
        return m_upper;
    }

    static <T> BoundedObjectDomain<T> empty() {
        @SuppressWarnings("unchecked")
        final BoundedObjectDomain<T> empty = (BoundedObjectDomain<T>)EMPTY;
        return empty;
    }

    /**
     * Calculates a {@link BoundedObjectDomain}
     *
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @param <T> type the objects
     */
    public final static class BoundedDomainCalculator<T>
        implements DomainCalculator<ObjectReadData<T>, BoundedObjectDomain<T>> {

        private final BoundedObjectDomainMerger<T> m_merger;

        private final Comparator<T> m_comparator;

        /**
         * Create a new calculator without initial domain
         *
         * @param comparator used to compare values
         */
        public BoundedDomainCalculator(final Comparator<T> comparator) {
            this(null, comparator);
        }

        /**
         * Create a new calculator with initial domain
         *
         * @param initial the initial domain
         * @param comparator used to order values.
         */
        public BoundedDomainCalculator(final BoundedObjectDomain<T> initial, //
            final Comparator<T> comparator) {
            m_merger = new BoundedObjectDomainMerger<>(initial, comparator);
            m_comparator = comparator;
        }

        @Override
        public BoundedObjectDomain<T> createInitialDomain() {
            return m_merger.createInitialDomain();
        }

        @Override
        public BoundedObjectDomain<T> calculateDomain(final ObjectReadData<T> data) {

            T lower = null;
            T upper = null;

            final int length = data.length();
            for (int i = 0; i < length; ++i) {
                if (!data.isMissing(i++)) {
                    final T other = data.getObject(i);
                    if (lower == null) {
                        lower = other;
                        upper = other;
                    } else {
                        if (m_comparator.compare(other, lower) == -1) {
                            lower = other;
                        } else if (m_comparator.compare(other, upper) > 0) {
                            upper = other;
                        }
                    }
                }
            }
            return new BoundedObjectDomain<>(lower, upper);
        }

        @Override
        public BoundedObjectDomain<T> mergeDomains(final BoundedObjectDomain<T> original,
            final BoundedObjectDomain<T> additional) {
            return m_merger.mergeDomains(original, additional);
        }
    }

    private static final class BoundedObjectDomainMerger<T> extends AbstractDomainMerger<BoundedObjectDomain<T>> {

        private final Comparator<T> m_comparator;

        public BoundedObjectDomainMerger(final BoundedObjectDomain<T> initialDomain, final Comparator<T> comparator) {
            super(initialDomain != null ? initialDomain : BoundedObjectDomain.empty());
            m_comparator = comparator;
        }

        @Override
        public BoundedObjectDomain<T> mergeDomains(final BoundedObjectDomain<T> original,
            final BoundedObjectDomain<T> additional) {
            return new BoundedObjectDomain<>(
                m_comparator.compare(original.m_lower, additional.m_lower) == -1 ? original.m_lower
                    : additional.m_lower,
                m_comparator.compare(original.m_upper, additional.m_upper) == -1 ? original.m_upper
                    : additional.m_upper);
        }
    }
}