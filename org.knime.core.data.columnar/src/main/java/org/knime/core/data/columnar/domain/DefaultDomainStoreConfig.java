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
import java.util.function.Supplier;

import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.data.BoundedValue;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValueComparatorDelegator;
import org.knime.core.data.NominalValue;
import org.knime.core.data.columnar.schema.ColumnarReadValueFactory;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.meta.DataColumnMetaDataCreator;
import org.knime.core.data.meta.DataColumnMetaDataRegistry;
import org.knime.core.util.DuplicateChecker;

/**
 * Config for {@link DomainColumnStore}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class DefaultDomainStoreConfig implements DomainStoreConfig {

    // factory for native domain calculators
    private final Map<DataType, Supplier<ColumnarDomainCalculator<? extends ColumnReadData, DataColumnDomain>>> m_nativeDomainCalculators;

    private final ColumnarValueSchema m_schema;

    private final int m_maxNumValues;

    private final boolean m_initDomains;

    private final boolean m_checkForDuplicates;

    private Map<Integer, ColumnarDomainCalculator<? extends ColumnReadData, DataColumnMetaData[]>> m_metadataCalculators;

    private Map<Integer, ColumnarDomainCalculator<? extends ColumnReadData, DataColumnDomain>> m_domainCalculators;

    /**
     * Create a new {@link DefaultDomainStoreConfig}
     *
     * @param schema the schema used to determine the column configuration.
     * @param maxPossibleNominalDomainValues
     * @param checkForDuplicates <source>true</source> if row key should be checked for duplicates
     * @param initializeDomains <source>true</source> if incoming domains/metadata should be used for initialization.
     */
    public DefaultDomainStoreConfig(final ColumnarValueSchema schema, final int maxPossibleNominalDomainValues,
        final boolean checkForDuplicates, final boolean initializeDomains) {
        m_schema = schema;
        m_initDomains = initializeDomains;
        m_checkForDuplicates = checkForDuplicates;
        m_maxNumValues = maxPossibleNominalDomainValues;
        m_nativeDomainCalculators = new HashMap<>();

        initNativeDomainCalculators();
    }

    private void initNativeDomainCalculators() {
        // TODO Provide extension point for expert users.
        m_nativeDomainCalculators.put(DoubleCell.TYPE, () -> new ColumnarDoubleDomainCalculator());
        m_nativeDomainCalculators.put(IntCell.TYPE, () -> new ColumnarIntDomainCalculator());
        m_nativeDomainCalculators.put(LongCell.TYPE, () -> new ColumnarLongDomainCalculator());
        //        m_nativeDomainCalculators.put(BooleanCell.TYPE, () -> new ColumnarDoubleDomainCalculator());
        m_nativeDomainCalculators.put(StringCell.TYPE, () -> new ColumnarStringDomainCalculator(m_maxNumValues));
    }

    // to make java happy
    @SuppressWarnings("unchecked")
    private static final void merge(@SuppressWarnings("rawtypes") final DataColumnMetaDataCreator m,
        final DataColumnMetaData o) {
        m.merge(o);
    }

    @Override
    public DomainStoreConfig withMaxPossibleNominalDomainValues(final int maxPossibleValues) {
        return new DefaultDomainStoreConfig(m_schema, maxPossibleValues, m_checkForDuplicates, m_initDomains);
    }

    @Override
    public DuplicateChecker createDuplicateChecker() {
        return m_checkForDuplicates ? new DuplicateChecker() : null;
    }

    // also initializes with incoming DataTableSpec
    @Override
    public Map<Integer, ColumnarDomainCalculator<? extends ColumnReadData, DataColumnDomain>>
        createDomainCalculators() {
        if (m_domainCalculators == null) {
            final int length = m_schema.getNumColumns();
            final DataTableSpec spec = m_schema.getSourceSpec();
            m_domainCalculators = new HashMap<>();
            final ColumnarReadValueFactory<?>[] factories = m_schema.getReadValueFactories();
            for (int i = 1; i < length; i++) {
                final DataType type = spec.getColumnSpec(i - 1).getType();

                final ColumnarDomainCalculator<? extends ColumnReadData, DataColumnDomain> calculator;
                // check for native (=faster) implementations
                if (m_nativeDomainCalculators.containsKey(type)) {
                    calculator = m_nativeDomainCalculators.get(type).get();
                } else {
                    // use our fallback implementations
                    final boolean isNominal = type.isCompatible(NominalValue.class);
                    final boolean isBounded = type.isCompatible(BoundedValue.class);
                    if (isNominal && isBounded) {
                        calculator = new ColumnarCombinedDomainCalculator<>(factories[i],
                            new DataValueComparatorDelegator<>(type.getComparator()), m_maxNumValues);
                    } else if (isNominal) {
                        calculator = new ColumnarNominalDomainCalculator<>(factories[i], m_maxNumValues);
                    } else if (isBounded) {
                        calculator = new ColumnarBoundedDomainCalculator<>(factories[i],
                            new DataValueComparatorDelegator<>(type.getComparator()));
                    } else {
                        calculator = null;
                    }
                }

                if (calculator != null) {
                    calculator.update(spec.getColumnSpec(i - 1).getDomain());
                    m_domainCalculators.put(i, calculator);
                }
            }
        }
        return m_domainCalculators;

    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Integer, ColumnarDomainCalculator<? extends ColumnReadData, DataColumnMetaData[]>>

        createMetadataCalculators() {
        if (m_metadataCalculators == null) {
            m_metadataCalculators = new HashMap<>();
            final int length = m_schema.getNumColumns();
            final DataTableSpec spec = m_schema.getSourceSpec();
            final ColumnarReadValueFactory<?>[] factories = m_schema.getReadValueFactories();
            for (int i = 1; i < length; i++) {
                final DataColumnSpec colSpec = spec.getColumnSpec(i - 1);
                final Collection<DataColumnMetaDataCreator<?>> metadataCreators =
                    DataColumnMetaDataRegistry.INSTANCE.getCreators(colSpec.getType());
                if (!metadataCreators.isEmpty()) {
                    if (m_initDomains) {
                        metadataCreators
                            .forEach(m -> colSpec.getMetaDataOfType(m.getMetaDataClass()).ifPresent(o -> merge(m, o)));
                        m_metadataCalculators.put(i,
                            new ColumnarMetadataDomainCalculator<>(
                                metadataCreators.toArray(new DataColumnMetaDataCreator[metadataCreators.size()]),
                                (ColumnarReadValueFactory<ColumnReadData>)factories[i]));
                    }
                }
            }
        }
        return m_metadataCalculators;
    }
}
