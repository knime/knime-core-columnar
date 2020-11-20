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

import java.util.LinkedHashSet;
import java.util.Set;

import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.def.BooleanCell;

final class ColumnarBooleanDomainCalculator implements ColumnarDomainCalculator<BooleanReadData, DataColumnDomain> {

    private Set<Boolean> m_values;

    public ColumnarBooleanDomainCalculator() {
        m_values = new LinkedHashSet<>();
    }

    @Override
    public final void update(final BooleanReadData data) {
        if (m_values.size() == 2) {
            return;
        }
        final int length = data.length();
        for (int i = 0; i < length; i++) {
            if (!data.isMissing(i)) {
                // TODO that's overly expensive for a simple boolean. However, we use a LinkedHashSet to keep the values in order of appearance.
                if (m_values.add(data.getBoolean(i)) && m_values.size() == 2) {
                    return;
                }
            }
        }
    }

    @Override
    public DataColumnDomain getDomain() {
        if (m_values.size() == 0) {
            return new DataColumnDomainCreator().createDomain();
        } else {
            final DataCell[] asArray =
                m_values.stream().map((b) -> (b ? BooleanCell.TRUE : BooleanCell.FALSE)).toArray(DataCell[]::new);
            return m_values.size() == 1 ? new DataColumnDomainCreator(asArray, asArray[0], asArray[0]).createDomain()
                : new DataColumnDomainCreator(asArray, BooleanCell.FALSE, BooleanCell.TRUE).createDomain();
        }
    }

    @Override
    public void update(final DataColumnDomain domain) {
        if (m_values.size() == 2) {
            return;
        }
        if (domain.hasValues()) {
            for (final DataCell value : domain.getValues()) {
                if (m_values.add(((BooleanValue)value).getBooleanValue()) && m_values.size() == 2) {
                    return;
                }
            }
        }
    }
}
