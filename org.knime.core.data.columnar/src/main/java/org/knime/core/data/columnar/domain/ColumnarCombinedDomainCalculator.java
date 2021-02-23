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
 *   Nov 3, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.domain;

import java.util.Comparator;

import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.data.BoundedValue;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataValue;
import org.knime.core.data.NominalValue;
import org.knime.core.data.columnar.schema.ColumnarReadValueFactory;

/**
 * Columnar domain calculator for arbitrary data that is both {@link BoundedValue bounded} and {@link NominalValue
 * nominal}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class ColumnarCombinedDomainCalculator<R extends NullableReadData>
    implements ColumnarDomainCalculator<R, DataColumnDomain> {

    private final ColumnarNominalDomainCalculator<R> m_nominal;

    private final ColumnarBoundedDomainCalculator<R> m_bounded;

    ColumnarCombinedDomainCalculator(final ColumnarReadValueFactory<R> factory, final Comparator<DataValue> delegate,
        final int maxNumValues) {
        m_nominal = new ColumnarNominalDomainCalculator<>(factory, maxNumValues);
        m_bounded = new ColumnarBoundedDomainCalculator<>(factory, delegate);
    }

    @Override
    public void update(final R data) {
        m_nominal.update(data);
        m_bounded.update(data);
    }

    @Override
    public void update(final DataColumnDomain domain) {
        m_nominal.update(domain);
        // calling update here would lead to values being stored redundantly, once in m_nominal and once in m_bounded
        m_bounded.updateBounds(domain);
    }

    @Override
    public DataColumnDomain createDomain() {
        final DataColumnDomain boundedDomain = m_bounded.createDomain();
        return new DataColumnDomainCreator(m_nominal.createDomain().getValues(), boundedDomain.getLowerBound(),
            boundedDomain.getUpperBound()).createDomain();
    }

}
