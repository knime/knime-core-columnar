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
 */
package org.knime.core.data.columnar.domain;

import java.util.Comparator;

import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.domain.BoundedDomain;
import org.knime.core.columnar.domain.DomainCalculator;
import org.knime.core.columnar.domain.DomainMerger;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataValue;
import org.knime.core.data.columnar.domain.BoundedDataValueDomainMapper.BoundedDataValueDomain;
import org.knime.core.data.columnar.domain.BoundedDataValueDomainMapper.BoundedDataValueDomain.BoundedDataValueDomainCalculator;
import org.knime.core.data.columnar.schema.ColumnarReadValueFactory;

final class BoundedDataValueDomainMapper<D extends DataValue>
    implements DomainFactory<ColumnReadData, BoundedDataValueDomain<D>> {

    private final Comparator<D> m_comparator;

    private final ColumnarReadValueFactory<ColumnReadData> m_factory;

    public BoundedDataValueDomainMapper( //
        final Comparator<D> comparator, final ColumnarReadValueFactory<ColumnReadData> factory) {
        m_comparator = comparator;
        m_factory = factory;
    }

    @Override
    public DomainCalculator<ColumnReadData, BoundedDataValueDomain<D>>
        createCalculator(final DataColumnDomain initialDomain) {
        final BoundedDataValueDomain<D> domain;
        if (initialDomain != null && initialDomain.hasBounds()) {
            @SuppressWarnings("unchecked")
            final D lower = (D)initialDomain.getLowerBound();
            @SuppressWarnings("unchecked")
            final D upper = (D)initialDomain.getUpperBound();
            domain = new BoundedDataValueDomain<>(lower, upper);
        } else {
            domain = new BoundedDataValueDomain<>();
        }
        return new BoundedDataValueDomainCalculator<>(domain, m_comparator, m_factory);
    }

    @Override
    public DataColumnDomain convert(final BoundedDataValueDomain<D> domain) {
        return new DataColumnDomainCreator((DataCell)domain.getLowerBound(), (DataCell)domain.getUpperBound())
            .createDomain();
    }

    final static class BoundedDataValueDomain<D extends DataValue> implements BoundedDomain<D> {

        private static final BoundedDataValueDomain<?> EMPTY = new BoundedDataValueDomain<>();

        private final D m_lower;

        private final D m_upper;

        public BoundedDataValueDomain() {
            this(null, null);
        }

        public BoundedDataValueDomain(final D lower, final D upper) {
            m_lower = lower;
            m_upper = upper;
        }

        @Override
        public boolean hasLowerBound() {
            return m_lower != null;
        }

        @Override
        public D getLowerBound() {
            return m_lower;
        }

        @Override
        public boolean hasUpperBound() {
            return m_upper != null;
        }

        @Override
        public D getUpperBound() {
            return m_upper;
        }

        static <D extends DataValue> BoundedDataValueDomain<D> empty() {
            @SuppressWarnings("unchecked")
            final BoundedDataValueDomain<D> empty = (BoundedDataValueDomain<D>)EMPTY;
            return empty;
        }

        private static final class BoundedObjectDomainMerger<D extends DataValue>
            implements DomainMerger<BoundedDataValueDomain<D>> {

            private final BoundedDataValueDomain<D> m_initialDomain;

            private final Comparator<D> m_comparator;

            public BoundedObjectDomainMerger(final BoundedDataValueDomain<D> initialDomain,
                final Comparator<D> comparator) {
                m_comparator = comparator;
                m_initialDomain = initialDomain;
            }

            @Override
            public BoundedDataValueDomain<D> mergeDomains(final BoundedDataValueDomain<D> original,
                final BoundedDataValueDomain<D> additional) {
                if (original.m_lower == null || original.m_upper == null) {
                    return new BoundedDataValueDomain<>(additional.m_lower, additional.m_upper);
                } else {
                    return new BoundedDataValueDomain<>(
                        m_comparator.compare(original.m_lower, additional.m_lower) == -1 ? original.m_lower
                            : additional.m_lower,
                        m_comparator.compare(original.m_upper, additional.m_upper) == -1 ? original.m_upper
                            : additional.m_upper);
                }
            }

            @Override
            public BoundedDataValueDomain<D> createInitialDomain() {
                return m_initialDomain;
            }
        }

        public final static class BoundedDataValueDomainCalculator<D extends DataValue>
            implements DomainCalculator<ColumnReadData, BoundedDataValueDomain<D>> {

            private final BoundedObjectDomainMerger<D> m_merger;

            private final Comparator<D> m_comparator;

            private final ColumnarReadValueFactory<ColumnReadData> m_factory;

            public BoundedDataValueDomainCalculator(final BoundedDataValueDomain<D> initial, //
                final Comparator<D> comparator, final ColumnarReadValueFactory<ColumnReadData> factory) {
                m_merger = new BoundedObjectDomainMerger<>(initial, comparator);
                m_comparator = comparator;
                m_factory = factory;
            }

            @Override
            public BoundedDataValueDomain<D> createInitialDomain() {
                return m_merger.createInitialDomain();
            }

            @Override
            public BoundedDataValueDomain<D> calculateDomain(final ColumnReadData data) {

                D lower = null;
                D upper = null;

                CopyableReadValueCursor cursor = new CopyableReadValueCursor(m_factory, data);

                while (cursor.canForward()) {
                    cursor.forward();
                    if (!cursor.isMissing()) {
                        final D other = cursor.copy();
                        if (lower == null) {
                            lower = other;
                            upper = other;
                        } else {
                            if (m_comparator.compare(other, lower) < 0) {
                                lower = cursor.copy();
                            } else if (m_comparator.compare(other, upper) > 0) {
                                upper = cursor.copy();
                            }
                        }
                    }
                }
                return new BoundedDataValueDomain<D>(lower, upper);
            }

            @Override
            public BoundedDataValueDomain<D> mergeDomains(final BoundedDataValueDomain<D> original,
                final BoundedDataValueDomain<D> additional) {
                return m_merger.mergeDomains(original, additional);
            }
        }
    }
}
