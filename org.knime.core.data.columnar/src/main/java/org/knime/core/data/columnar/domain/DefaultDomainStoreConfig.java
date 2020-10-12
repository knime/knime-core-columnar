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

import java.util.HashMap;
import java.util.Map;

import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.domain.Domain;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
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

    private final DomainFactoryMapper m_mapper;

    private final Map<Integer, DataColumnDomain> m_initialDomains;

    /**
     * Create a new {@link DefaultDomainStoreConfig}
     *
     * @param schema the schema used to determine the column configuration.
     * @param maxPossibleNominalDomainValues
     * @param rowKeyConfig
     */
    public DefaultDomainStoreConfig(final ColumnarValueSchema schema, final int maxPossibleNominalDomainValues,
        final RowKeyType rowKeyConfig) {
        m_schema = schema;
        m_rowKeyConfig = rowKeyConfig;
        m_mapper = new DomainFactoryMapper(schema, maxPossibleNominalDomainValues);
        m_initialDomains = new HashMap<>();

        final int length = schema.getNumColumns();
        final DataTableSpec spec = schema.getSourceSpec();
        for (int i = 1; i < length; i++) {
            final DataColumnDomain domain = spec.getColumnSpec(i - 1).getDomain();
            if (domain.hasBounds() || domain.hasValues()) {
                m_initialDomains.put(i, domain);
            }
        }
    }

    @Override
    public DomainStoreConfig withMaxPossibleNominalDomainValues(final int maxPossibleValues) {
        return new DefaultDomainStoreConfig(m_schema, maxPossibleValues, m_rowKeyConfig);
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

    @Override
    public Map<Integer, DomainFactory<? extends ColumnReadData, ? extends Domain>> createMappers() {
        return m_mapper.createDomainFactories();
    }

    @Override
    public Map<Integer, DataColumnDomain> getInitialDomains() {
        return m_initialDomains;
    }
}
