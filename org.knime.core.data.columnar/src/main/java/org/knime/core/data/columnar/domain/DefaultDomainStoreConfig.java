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

import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.domain.Domain;
import org.knime.core.columnar.domain.DomainCalculator;
import org.knime.core.data.BoundedValue;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.DataValueComparatorDelegator;
import org.knime.core.data.NominalValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.meta.DataColumnMetaDataCreator;
import org.knime.core.data.meta.DataColumnMetaDataRegistry;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.util.DuplicateChecker;

/**
 * Config for {@link DomainColumnStore}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class DefaultDomainStoreConfig implements DomainStoreConfig {

    private final ColumnarValueSchema m_schema;

    private final RowKeyType m_rowKeyConfig;

    private final int m_maxNumValues;

    private final boolean m_initDomains;

    private Map<Integer, DomainCalculator<? extends ColumnReadData, DataColumnMetaData[]>> m_metadataCalculators;

    private Map<Integer, DomainCalculator<? extends ColumnReadData, DataColumnDomain>> m_domainCalculators;

    /**
     * Create a new {@link DefaultDomainStoreConfig}
     *
     * @param schema the schema used to determine the column configuration.
     * @param maxPossibleNominalDomainValues
     * @param rowKeyConfig
     * @param initializeDomains <source>true</source> if incoming domains/metadata should be used for initialization.
     */
    public DefaultDomainStoreConfig(final ColumnarValueSchema schema, final int maxPossibleNominalDomainValues,
        final RowKeyType rowKeyConfig, final boolean initializeDomains) {
        m_schema = schema;
        m_initDomains = initializeDomains;
        m_rowKeyConfig = rowKeyConfig;
        m_maxNumValues = maxPossibleNominalDomainValues;
    }

    // to make java happy
    @SuppressWarnings("unchecked")
    private static final void merge(@SuppressWarnings("rawtypes") final DataColumnMetaDataCreator m,
        final DataColumnMetaData o) {
        m.merge(o);
    }

    @Override
    public DomainStoreConfig withMaxPossibleNominalDomainValues(final int maxPossibleValues) {
        return new DefaultDomainStoreConfig(m_schema, maxPossibleValues, m_rowKeyConfig, m_initDomains);
    }

    @Override
    public DuplicateChecker createDuplicateChecker() {
        final DuplicateChecker duplicateChecker;
        switch (m_rowKeyConfig) {
            case CUSTOM:
                duplicateChecker = new DuplicateChecker();
                break;
            case NOKEY:
                duplicateChecker = null;
                break;
            default:
                throw new IllegalStateException("Unknown row key type: " + m_rowKeyConfig.name());
        }
        return duplicateChecker;
    }

    // also initializes with incoming DataTableSpec
    @Override
    public Map<Integer, DomainCalculator<? extends ColumnReadData, DataColumnDomain>> createDomainCalculators() {
        if (m_domainCalculators == null) {
            final int length = m_schema.getNumColumns();
            m_domainCalculators = new HashMap<>();
            final DataTableSpec spec = m_schema.getSourceSpec();

            for (int i = 1; i < length; i++) {
                final DataColumnSpec colSpec = spec.getColumnSpec(i - 1);

                // try to find the factory
                DomainFactory<?, ?> factory = m_schema.getColumnDataSpec(i).accept(DomainFactoryMapper.INSTANCE);
                if (factory == null) {
                    final DataType type = spec.getColumnSpec(i - 1).getType();
                    if (type.isCompatible(BoundedValue.class)) {
                        factory = new BoundedDataValueDomainFactory<DataValue>(
                            new DataValueComparatorDelegator<>(type.getComparator()),
                            m_schema.getReadValueFactoryAt(i));
                    } else if (type.isCompatible(NominalValue.class)) {
                        factory =
                            new NominalDataValueDomainFactory<>(m_maxNumValues, m_schema.getReadValueFactoryAt(i));
                    }
                }

                // we did all we could to find the right factory
                if (factory != null) {
                    final DataColumnDomain domain;
                    if (m_initDomains) {
                        domain = colSpec.getDomain();
                    } else {
                        domain = new DataColumnDomainCreator().createDomain();
                    }
                    m_domainCalculators.put(i, new ConvertingDomainCalculator<>(factory, domain));
                }
            }
        }

        return m_domainCalculators;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Integer, DomainCalculator<? extends ColumnReadData, DataColumnMetaData[]>> createMetadataCalculators() {
        if (m_metadataCalculators == null) {
            m_metadataCalculators = new HashMap<>();
            final int length = m_schema.getNumColumns();
            final DataTableSpec spec = m_schema.getSourceSpec();
            for (int i = 1; i < length; i++) {
                final DataColumnSpec colSpec = spec.getColumnSpec(i - 1);
                final Collection<DataColumnMetaDataCreator<?>> metadataCreators =
                    DataColumnMetaDataRegistry.INSTANCE.getCreators(colSpec.getType());
                if (!metadataCreators.isEmpty()) {
                    if (m_initDomains) {
                        metadataCreators
                            .forEach(m -> colSpec.getMetaDataOfType(m.getMetaDataClass()).ifPresent(o -> merge(m, o)));
                        m_metadataCalculators.put(i,
                            new MetadataDomainCalculator<>(
                                metadataCreators.toArray(new DataColumnMetaDataCreator[metadataCreators.size()]),
                                m_schema.getReadValueFactoryAt(i)));
                    }
                }
            }
        }
        return m_metadataCalculators;
    }

    private static final class ConvertingDomainCalculator<C extends ColumnReadData, D extends Domain>
        implements DomainCalculator<C, DataColumnDomain> {

        private final DomainCalculator<C, ? extends D> m_calculator;

        private final DomainFactory<C, D> m_factory;

        private ConvertingDomainCalculator(final DomainFactory<C, D> factory, final DataColumnDomain initial) {
            m_calculator = factory.createCalculator(initial);
            m_factory = factory;
        }

        @Override
        public void update(final C data) {
            m_calculator.update(data);
        }

        @Override
        public DataColumnDomain getDomain() {
            return m_factory.convert(m_calculator.getDomain());
        }
    }
}
