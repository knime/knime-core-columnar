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

import org.knime.core.columnar.ColumnDataIndex;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.columnar.schema.ColumnarReadValueFactory;
import org.knime.core.data.v2.ReadValue;

final class ColumnarNominalDomainCalculator<C extends ColumnReadData>
    implements ColumnarDomainCalculator<C, DataColumnDomain>, ColumnDataIndex {

    private final ColumnarReadValueFactory<C> m_factory;

    private final int m_maxNumValues;

    private Set<DataCell> m_values;

    private int m_index;

    public ColumnarNominalDomainCalculator(final ColumnarReadValueFactory<C> factory, final int maxNumvalues) {
        m_factory = factory;
        m_maxNumValues = maxNumvalues;
        m_values = new LinkedHashSet<>();
    }

    @Override
    public void update(final C data) {
        if (m_values == null) {
            return;
        }
        final ReadValue value = m_factory.createReadValue(data, this);
        final int length = data.length();
        for (int i = 0; i < length; i++) {
            if (!data.isMissing(i)) {
                m_index = i;
                m_values.add(value.getDataCell());

                if (m_values.size() > m_maxNumValues) {
                    m_values = null;
                    return;
                }
            }
        }
    }

    @Override
    public DataColumnDomain getDomain() {
        return m_values == null ? new DataColumnDomainCreator().createDomain()
            : new DataColumnDomainCreator(m_values).createDomain();
    }

    @Override
    public int getIndex() {
        return m_index;
    }

    @Override
    public void update(final DataColumnDomain domain) {
        if (domain.hasValues()) {
            for (final DataCell cell : domain.getValues()) {
                if (!cell.isMissing()) {
                    m_values.add(cell);
                    if (m_values.size() > m_maxNumValues) {
                        m_values = null;
                        return;
                    }
                }
            }
        }
    }
}
