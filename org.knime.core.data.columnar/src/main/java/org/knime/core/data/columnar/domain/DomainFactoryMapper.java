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

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.columnar.data.BooleanData.BooleanDataSpec;
import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.ByteData.ByteDataSpec;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleDataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DurationData.DurationDataSpec;
import org.knime.core.columnar.data.FloatData.FloatDataSpec;
import org.knime.core.columnar.data.IntData.IntDataSpec;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.ListData.ListDataSpec;
import org.knime.core.columnar.data.LocalDateData.LocalDateDataSpec;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeDataSpec;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeDataSpec;
import org.knime.core.columnar.data.LongData.LongDataSpec;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.ObjectData.ObjectDataSpec;
import org.knime.core.columnar.data.PeriodData.PeriodDataSpec;
import org.knime.core.columnar.data.StructData.StructDataSpec;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryDataSpec;
import org.knime.core.columnar.data.VoidData.VoidDataSpec;
import org.knime.core.columnar.domain.BooleanDomain;
import org.knime.core.columnar.domain.BooleanDomain.BooleanDomainCalculator;
import org.knime.core.columnar.domain.DomainCalculator;
import org.knime.core.columnar.domain.DoubleDomain;
import org.knime.core.columnar.domain.DoubleDomain.DoubleDomainCalculator;
import org.knime.core.columnar.domain.IntDomain;
import org.knime.core.columnar.domain.IntDomain.IntDomainCalculator;
import org.knime.core.columnar.domain.LongDomain;
import org.knime.core.columnar.domain.LongDomain.LongDomainCalculator;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;

/**
 * Utility class to map ColumnDataSpecs to DomainFactories.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz
 */
final class DomainFactoryMapper implements ColumnDataSpec.Mapper<DomainFactory<?, ?>> {

    final static DomainFactoryMapper INSTANCE = new DomainFactoryMapper();

    /**
     * Creates a new DomainMapper
     *
     * @param schema to derive DomainFactory from
     * @param maxValues maximum number of values of a nominal domain
     */
    private DomainFactoryMapper() {
    }

    /*
     * In case we don't have a specific domain mapper, we just return null and the general domain mapping mechanism kicks in.
     *
     * TODO discuss: We now calculate the domain for each extension backed, e.g. by double. Do we
     * (a) want the user to be able to disable domain calculation?
     * (b) restrict that to only DoubleCell.TYPE, IntCell.TYPE, etc
     * (c) just calculate the domain as doubles?
     *
     * Use-case: User decided to use DoubleAccess as backend for his/her own ValueFactory but modifies each value on 'read' e.g. by adding 5 to each written value.
     * The domain values would now differ from the actual values.
     */

    @Override
    public DomainFactory<?, ?> visit(final BooleanDataSpec spec) {
        return new BooleanDomainFactory();
    }

    @Override
    public DomainFactory<?, ?> visit(final DoubleDataSpec spec) {
        return new DoubleDomainFactory();
    }

    @Override
    public DomainFactory<?, ?> visit(final IntDataSpec spec) {
        return new IntDomainFactory();
    }

    @Override
    public DomainFactory<?, ?> visit(final LongDataSpec spec) {
        return new LongDomainFactory();
    }

    @Override
    public DomainFactory<?, ?> visit(final ByteDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final ObjectDataSpec<?> spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final VoidDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final DurationDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final FloatDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final LocalDateDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final LocalDateTimeDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final LocalTimeDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final PeriodDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final VarBinaryDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final StructDataSpec spec) {
        return null;
    }

    @Override
    public DomainFactory<?, ?> visit(final ListDataSpec listDataSpec) {
        return null;
    }

    final static class DoubleDomainFactory implements DomainFactory<DoubleReadData, DoubleDomain> {
        @Override
        public DomainCalculator<DoubleReadData, DoubleDomain> createCalculator(final DataColumnDomain domain) {
            if (domain.hasBounds()) {
                return new DoubleDomainCalculator(
                    new DoubleDomain(((DoubleValue)domain.getLowerBound()).getDoubleValue(), //
                        ((DoubleValue)domain.getUpperBound()).getDoubleValue()));
            } else {
                return new DoubleDomainCalculator();
            }
        }

        @Override
        public DataColumnDomain convert(final DoubleDomain domain) {
            if (domain.hasLowerBound() && domain.hasUpperBound()) {
                return new DataColumnDomainCreator(new DoubleCell(domain.getLowerBound()),
                    new DoubleCell(domain.getUpperBound())).createDomain();
            } else {
                return new DataColumnDomainCreator().createDomain();
            }
        }
    }

    final static class IntDomainFactory implements DomainFactory<IntReadData, IntDomain> {
        @Override
        public DomainCalculator<IntReadData, IntDomain> createCalculator(final DataColumnDomain domain) {
            if (domain.hasBounds()) {
                return new IntDomainCalculator(new IntDomain(((IntValue)domain.getLowerBound()).getIntValue(), //
                    ((IntValue)domain.getUpperBound()).getIntValue()));
            } else {
                return new IntDomainCalculator();
            }
        }

        @Override
        public DataColumnDomain convert(final IntDomain domain) {
            if (domain.hasLowerBound() && domain.hasUpperBound()) {
                return new DataColumnDomainCreator(new IntCell(domain.getLowerBound()),
                    new IntCell(domain.getUpperBound())).createDomain();
            } else {
                return new DataColumnDomainCreator().createDomain();
            }
        }
    }

    final static class LongDomainFactory implements DomainFactory<LongReadData, LongDomain> {
        @Override
        public DomainCalculator<LongReadData, LongDomain> createCalculator(final DataColumnDomain domain) {
            if (domain.hasBounds()) {
                return new LongDomainCalculator(new LongDomain(((LongValue)domain.getLowerBound()).getLongValue(), //
                    ((LongValue)domain.getUpperBound()).getLongValue()));
            } else {
                return new LongDomainCalculator();
            }
        }

        @Override
        public DataColumnDomain convert(final LongDomain domain) {
            if (domain.isValid()) {
                return new DataColumnDomainCreator(new LongCell(domain.getLowerBound()),
                    new LongCell(domain.getUpperBound())).createDomain();
            } else {
                return new DataColumnDomainCreator().createDomain();
            }
        }
    }

    final static class BooleanDomainFactory implements DomainFactory<BooleanReadData, BooleanDomain> {
        @Override
        public DomainCalculator<BooleanReadData, BooleanDomain> createCalculator(final DataColumnDomain domain) {
            if (domain.hasValues()) {
                final Set<Boolean> initial = Stream.of(domain.getValues())
                    .map((d) -> ((BooleanValue)d).getBooleanValue()).collect(Collectors.toSet());
                return new BooleanDomainCalculator(new BooleanDomain(initial));
            } else {
                return new BooleanDomainCalculator();
            }
        }

        @Override
        public DataColumnDomain convert(final BooleanDomain domain) {
            if (domain.isValid()) {
                final DataCell[] cells = domain.getValues().stream()
                    .map((b) -> b ? BooleanCell.TRUE : BooleanCell.FALSE).toArray(DataCell[]::new);
                if (cells.length == 1) {
                    return new DataColumnDomainCreator(cells, cells[0], cells[0]).createDomain();
                } else {
                    return new DataColumnDomainCreator(cells, cells[0], cells[1]).createDomain();
                }
            } else {
                return new DataColumnDomainCreator().createDomain();
            }
        }
    }

}
