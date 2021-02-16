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
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;

/**
 * Columnar domain calculator for {@link BooleanReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class ColumnarBooleanDomainCalculator implements ColumnarCalculator<BooleanReadData, DataColumnDomain> {

    private final Set<DataCell> m_values = new LinkedHashSet<>(2);

    @Override
    public final void update(final BooleanReadData data) {
        if (m_values.size() == 2) {
            return;
        }
        for (int i = 0; i < data.length(); i++) {
            if (!data.isMissing(i) && m_values.add(BooleanCellFactory.create(data.getBoolean(i)))
                && m_values.size() == 2) {
                return;
            }
        }
    }

    @Override
    public DataColumnDomain get() {
        switch (m_values.size()) {
            case 1:
                final DataCell cell = m_values.iterator().next();
                return new DataColumnDomainCreator(m_values, cell, cell).createDomain();
            case 2:
                return new DataColumnDomainCreator(m_values, BooleanCell.FALSE, BooleanCell.TRUE).createDomain();
            default:
                return new DataColumnDomainCreator(m_values).createDomain();
        }
    }

    @Override
    public void update(final DataColumnDomain domain) {
        if (m_values.size() == 2) {
            return;
        }
        if (domain.hasValues()) {
            for (final DataCell cell : domain.getValues()) {
                if (!cell.isMissing() && m_values.add(BooleanCellFactory.create(((BooleanValue)cell).getBooleanValue()))
                    && m_values.size() == 2) {
                    return;
                }
            }
        }
    }

}
