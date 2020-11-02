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
 *  KNIME longeroperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the longerpretation of the
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
 *  NodeDialog, and NodeView) and that only longeroperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for longeroperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for longeroperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Oct 31, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.domain;

import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.LongValue;
import org.knime.core.data.def.LongCell;

final class ColumnarLongDomainCalculator implements ColumnarDomainCalculator<LongReadData, DataColumnDomain> {

    private long m_lower = Long.MAX_VALUE;

    private long m_upper = Long.MIN_VALUE;

    @Override
    public final void update(final LongReadData data) {
        final int length = data.length();
        for (int i = 0; i < length; i++) {
            if (!data.isMissing(i)) {
                final long curr = data.getLong(i);
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
            return new DataColumnDomainCreator().createDomain();
        } else {
            return new DataColumnDomainCreator(new LongCell(m_lower), new LongCell(m_upper)).createDomain();
        }
    }

    @Override
    public void update(final DataColumnDomain domain) {
        if (domain.hasBounds()) {
            final long lower = ((LongValue)domain.getLowerBound()).getLongValue();
            if (m_lower > lower) {
                m_lower = lower;
            }

            final long upper = ((LongValue)domain.getUpperBound()).getLongValue();
            if (m_upper < upper) {
                m_upper = upper;
            }
        }
    }
}
