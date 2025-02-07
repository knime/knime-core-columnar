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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.data.BoundedValue;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValueComparatorDelegator;
import org.knime.core.data.NominalValue;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.meta.DataColumnMetaDataCreator;
import org.knime.core.data.meta.DataColumnMetaDataRegistry;
import org.knime.core.data.v2.schema.ValueSchema;

/**
 * Default Configuration for {@link DomainWritable}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class DefaultDomainWritableConfig implements DomainWritableConfig {

    @SuppressWarnings("unchecked")
    private static final void merge(@SuppressWarnings("rawtypes") final DataColumnMetaDataCreator m, // NOSONAR
        final DataColumnMetaData o) {
        m.merge(o);
    }

    // factory for native domain calculators
    private static final Map<DataType, //
            IntFunction<ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain>>> NATIVE_DOMAIN_CALCULATORS;
    static {
        NATIVE_DOMAIN_CALCULATORS = new HashMap<>(5);
        //bounded
        NATIVE_DOMAIN_CALCULATORS.put(IntCell.TYPE, n -> new ColumnarIntDomainCalculator());
        NATIVE_DOMAIN_CALCULATORS.put(LongCell.TYPE, n -> new ColumnarLongDomainCalculator());
        NATIVE_DOMAIN_CALCULATORS.put(DoubleCell.TYPE, n -> new ColumnarDoubleDomainCalculator());
        // nominal
        NATIVE_DOMAIN_CALCULATORS.put(StringCell.TYPE, ColumnarStringDomainCalculator::new);
        NATIVE_DOMAIN_CALCULATORS.put(BooleanCell.TYPE, n -> new ColumnarBooleanDomainCalculator());
    }

    private final ValueSchema m_schema;

    private final int m_maxNumValues;

    private final boolean m_initDomains;

    private final ColumnarReadValueFactory<?>[] m_readValueFactories;

    private Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnMetaData[]>> m_metadataCalculators;

    private Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain>> m_domainCalculators;

    /**
     * @param schema the schema used to determine the column configuration
     * @param maxPossibleNominalDomainValues the maximum number of values for nominal domains
     * @param initializeDomains <source>true</source> if incoming domains/metadata should be used for initialization
     */
    public DefaultDomainWritableConfig(final ValueSchema schema, final int maxPossibleNominalDomainValues,
        final boolean initializeDomains) {
        m_schema = schema;
        m_readValueFactories = IntStream.range(0, schema.numColumns())//
            .mapToObj(schema::getValueFactory)//
            .map(DefaultReadValueFactory::new)//
            .toArray(ColumnarReadValueFactory[]::new);
        m_initDomains = initializeDomains;
        m_maxNumValues = maxPossibleNominalDomainValues;
    }

    @Override
    public DomainWritableConfig withMaxPossibleNominalDomainValues(final int maxPossibleValues) {
        return new DefaultDomainWritableConfig(m_schema, maxPossibleValues, m_initDomains);
    }

    @Override
    public Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain>>
        createDomainCalculators() {
        if (m_domainCalculators == null) {
            m_domainCalculators = new ConcurrentHashMap<>();
            for (int i = 0; i < m_schema.numColumns(); i++) {
                final DataColumnSpec colSpec = m_schema.getDataColumnSpec(i);
                if (colSpec != null) {
                    final ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain> calculator =
                        createDomainCalculator(colSpec, m_readValueFactories[i]);
                    if (calculator != null) {
                        if (m_initDomains) { // NOSONAR
                            calculator.update(colSpec.getDomain());
                        }
                        m_domainCalculators.put(i, calculator);
                    }
                }
            }
        }
        return m_domainCalculators;
    }

    private ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain>
        createDomainCalculator(final DataColumnSpec colSpec, final ColumnarReadValueFactory<?> factory) {

        final DataType type = colSpec.getType();
        final DataColumnDomain domain = colSpec.getDomain();
        final int maxNumValues =
            m_initDomains && domain.hasValues() ? Math.max(m_maxNumValues, domain.getValues().size()) : m_maxNumValues;
        final IntFunction<ColumnarDomainCalculator<? extends NullableReadData, DataColumnDomain>> nativeDomainCalculator =
            NATIVE_DOMAIN_CALCULATORS.get(type);
        if (nativeDomainCalculator != null) {
            return nativeDomainCalculator.apply(maxNumValues);
        }

        final boolean isNominal = type.isCompatible(NominalValue.class);
        final boolean isBounded = type.isCompatible(BoundedValue.class);
        if (isNominal) {
            if (isBounded) {
                return new ColumnarCombinedDomainCalculator<>(factory,
                    new DataValueComparatorDelegator<>(type.getComparator()), maxNumValues);
            } else {
                return new ColumnarNominalDomainCalculator<>(factory, maxNumValues);
            }
        } else if (isBounded) {
            return new ColumnarBoundedDomainCalculator<>(factory,
                new DataValueComparatorDelegator<>(type.getComparator()));
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Integer, ColumnarDomainCalculator<? extends NullableReadData, DataColumnMetaData[]>>
        createMetadataCalculators() {
        if (m_metadataCalculators == null) {
            m_metadataCalculators = new ConcurrentHashMap<>();
            for (int i = 0; i < m_schema.numColumns(); i++) {
                final DataColumnSpec colSpec = m_schema.getDataColumnSpec(i);
                if (colSpec != null) {
                    final Collection<DataColumnMetaDataCreator<?>> metadataCreators =
                        DataColumnMetaDataRegistry.INSTANCE.getCreators(colSpec.getType());
                    if (!metadataCreators.isEmpty() && m_initDomains) {
                        metadataCreators
                            .forEach(m -> colSpec.getMetaDataOfType(m.getMetaDataClass()).ifPresent(o -> merge(m, o)));
                        m_metadataCalculators.put(i,
                            new ColumnarMetadataCalculator<>(
                                metadataCreators.toArray(new DataColumnMetaDataCreator[metadataCreators.size()]),
                                (ColumnarReadValueFactory<NullableReadData>)m_readValueFactories[i]));
                    }
                }
            }
        }
        return m_metadataCalculators;
    }

}
