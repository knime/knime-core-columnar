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
 *   Sep 30, 2020 (marcel): created
 */
package org.knime.core.columnar.domain;

import java.util.HashSet;
import java.util.Set;

import org.knime.core.columnar.data.BooleanData.BooleanReadData;

/**
 * A boolean domain is both bounded and nominal.
 *
 * @since 4.3
 */
public final class BooleanDomain extends AbstractNominalDomain<Boolean> implements BoundedDomain<Boolean> {

    /** Empty boolean domain **/
    public static final BooleanDomain EMPTY = new BooleanDomain();

    private BooleanDomain() {
    }

    /**
     * @param values The set of nominal values described by this domain. Can be {@code null} in which case this domain
     *            will be {@link #isValid() invalid}. An empty set will create a valid, empty domain (however, in this
     *            case, using {@link #EMPTY} should be preferred instead to avoid object creation).
     */
    public BooleanDomain(final Set<Boolean> values) {
        super(values);
    }

    @Override
    public boolean hasLowerBound() {
        return !m_values.isEmpty();
    }

    @Override
    public Boolean getLowerBound() {
        if (m_values.size() == 2) {
            return false;
        } else if (m_values.size() == 1) {
            return m_values.iterator().next();
        } else {
            return null;
        }
    }

    @Override
    public boolean hasUpperBound() {
        return !m_values.isEmpty();
    }

    @Override
    public Boolean getUpperBound() {
        if (m_values.size() == 2) {
            return true;
        } else if (m_values.size() == 1) {
            return m_values.iterator().next();
        } else {
            return null;
        }
    }

    /**
     * Calculates a new {@link BooleanDomain}.
     *
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     */
    public static final class BooleanDomainCalculator implements DomainCalculator<BooleanReadData, BooleanDomain> {

        private final Set<Boolean> m_values;

        /**
         * Create new {@link BooleanDomainCalculator} with no initial domain
         */
        public BooleanDomainCalculator() {
            m_values = new HashSet<>();
        }

        /**
         * Create new {@link BooleanDomainCalculator} with no initial domain
         *
         * @param init the initial domain
         */
        public BooleanDomainCalculator(final BooleanDomain init) {
            m_values = new HashSet<>(init.getValues());
        }

        @Override
        public final void update(final BooleanReadData data) {
            if (m_values.size() == 2) {
                return;
            } else {
                for (int i = 0; i < data.length(); i++) {
                    if (!data.isMissing(i)) {
                        m_values.add(data.getBoolean(i));
                    }
                    if (m_values.size() == 2) {
                        break;
                    }
                }
            }
        }

        @Override
        public BooleanDomain getDomain() {
            return new BooleanDomain(m_values);
        }
    }
}
