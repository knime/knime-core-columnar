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
 *   Oct 7, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.domain;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.domain.AbstractNominalDomain;
import org.knime.core.columnar.domain.DomainCalculator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataValue;
import org.knime.core.data.columnar.domain.NominalDataValueDomainFactory.NominalObjectDomain;
import org.knime.core.data.columnar.schema.ColumnarReadValueFactory;

final class NominalDataValueDomainFactory<D extends DataValue>
    implements DomainFactory<ColumnReadData, NominalObjectDomain<D>> {

    private final int m_maxValues;

    private final ColumnarReadValueFactory<ColumnReadData> m_factory;

    public NominalDataValueDomainFactory(final int maxValues, final ColumnarReadValueFactory<ColumnReadData> factory) {
        m_maxValues = maxValues;
        m_factory = factory;
    }

    @Override
    public DomainCalculator<ColumnReadData, ? extends NominalObjectDomain<D>>
        createCalculator(final DataColumnDomain initial) {
        if (initial.hasValues()) {
            @SuppressWarnings("unchecked")
            final NominalObjectDomain<D> domain = new NominalObjectDomain<D>((Set<D>)initial.getValues());
            return new NominalObjectDomainCalculator<>(domain, m_maxValues, m_factory);
        } else {
            return new NominalObjectDomainCalculator<>(m_maxValues, m_factory);
        }

    }

    @Override
    public DataColumnDomain convert(final NominalObjectDomain<D> domain) {
        if (domain.isValid()) {
            final Set<D> values = domain.getValues();
            final DataCell[] cells = new DataCell[values.size()];
            final Iterator<D> iterator = values.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                cells[i++] = (DataCell)iterator.next();
            }
            return new DataColumnDomainCreator(cells).createDomain();
        } else {
            return new DataColumnDomainCreator().createDomain();
        }
    }

    final static class NominalObjectDomainCalculator<D extends DataValue>
        implements DomainCalculator<ColumnReadData, NominalObjectDomain<D>> {

        private final int m_numMaxValues;

        private final ColumnarReadValueFactory<ColumnReadData> m_factory;

        private Set<D> m_values;

        NominalObjectDomainCalculator(final int numMaxValues, final ColumnarReadValueFactory<ColumnReadData> factory) {
            this(NominalObjectDomain.empty(), numMaxValues, factory);
        }

        NominalObjectDomainCalculator(final NominalObjectDomain<D> initial, final int numMaxValues,
            final ColumnarReadValueFactory<ColumnReadData> factory) {
            m_numMaxValues = numMaxValues;
            m_factory = factory;

            m_values = new LinkedHashSet<>();
            if (initial != null) {
                for (final D cell : initial.getValues()) {
                    m_values.add(cell);
                }
            }
        }

        @Override
        public void update(final ColumnReadData data) {
            if (m_values == null) {
                return;
            }
            final CopyableReadValueCursor cursor = new CopyableReadValueCursor(m_factory, data);

            while (cursor.canForward()) {
                cursor.forward();
                if (!cursor.isMissing()) {
                    // TODO can we avoid the copy here but still get backwards compatible results?
                    @SuppressWarnings("unchecked")
                    final D cast = (D)cursor.copy();
                    m_values.add(cast);
                    if (m_values.size() > m_numMaxValues) {
                        // Null indicates that domain could not be computed due to excessive
                        // distinct elements. Computed domain will be marked invalid.
                        m_values = null;
                        break;
                    }
                }
            }
        }

        @Override
        public NominalObjectDomain<D> getDomain() {
            if (m_values != null) {
                return new NominalObjectDomain<>(m_values);
            } else {
                return new NominalObjectDomain<>();
            }
        }
    }

    final static class NominalObjectDomain<D extends DataValue> extends AbstractNominalDomain<D> {

        private static final NominalObjectDomain<?> EMPTY = new NominalObjectDomain<>();

        public NominalObjectDomain(final Set<D> values) {
            super(values);
        }

        private NominalObjectDomain() {
            super();
        }

        static <D extends DataValue> NominalObjectDomain<D> empty() {
            @SuppressWarnings("unchecked")
            final NominalObjectDomain<D> empty = (NominalObjectDomain<D>)EMPTY;
            return empty;
        }
    }
}
