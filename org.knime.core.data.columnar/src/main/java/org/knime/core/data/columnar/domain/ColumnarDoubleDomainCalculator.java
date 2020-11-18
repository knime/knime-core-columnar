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
 *   Oct 31, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.domain;

import java.util.LinkedHashSet;
import java.util.Set;

import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.DoubleCell;

final class ColumnarDoubleDomainCalculator implements ColumnarDomainCalculator<DoubleReadData, DataColumnDomain> {

    private double m_lower = Double.POSITIVE_INFINITY;

    private double m_upper = Double.NEGATIVE_INFINITY;

    // ignored during update with IntReadData, but used in case domain was initialized with possible values
    private final Set<DataCell> m_values = new LinkedHashSet<>();

    @Override
    public final void update(final DoubleReadData data) {
        final int length = data.length();
        for (int i = 0; i < length; i++) {
            if (!data.isMissing(i)) {
                final double curr = data.getDouble(i);
                if (m_lower > curr) {
                    m_lower = curr;
                }
                if (m_upper < curr) {
                    m_upper = curr;
                }
            }
        }
    }

    @Override
    public DataColumnDomain getDomain() {
        if (m_lower > m_upper) {
            return new DataColumnDomainCreator(m_values.size() == 0 ? null : m_values).createDomain();
        } else {
            return new DataColumnDomainCreator(m_values.size() == 0 ? null : m_values, new DoubleCell(m_lower),
                new DoubleCell(m_upper)).createDomain();
        }
    }

    @Override
    public void update(final DataColumnDomain domain) {
        if (domain.hasBounds()) {
            final DataCell lowerBound = domain.getLowerBound();
            final DataCell upperBound = domain.getUpperBound();
            if (!lowerBound.isMissing() && !upperBound.isMissing()) {
                final double lower = ((DoubleValue)lowerBound).getDoubleValue();
                if (m_lower > lower) {
                    m_lower = lower;
                }

                final double upper = ((DoubleValue)upperBound).getDoubleValue();
                if (m_upper < upper) {
                    m_upper = upper;
                }
            }
        }

        if (domain.hasValues()) {
            m_values.addAll(domain.getValues());
        }
    }
}
