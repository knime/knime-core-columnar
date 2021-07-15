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
 *   Oct 6, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.schema;

import java.util.Iterator;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.traits.DataTraits;

/**
 * {@link ColumnarValueSchema} that is based on another schema, but has an updated {@link DataTableSpec}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class UpdatedColumnarValueSchema implements ColumnarValueSchema {

    private final DataTableSpec m_updatedSpec;

    private final ColumnarValueSchema m_delegate;

    UpdatedColumnarValueSchema(final DataTableSpec spec, final ColumnarValueSchema delegate) {
        m_updatedSpec = spec;
        m_delegate = delegate;
    }

    @Override
    public int numColumns() {
        return m_delegate.numColumns();
    }

    @Override
    public DataSpec getSpec(final int index) {
        return m_delegate.getSpec(index);
    }

    @Override
    public DataTableSpec getSourceSpec() {
        return m_updatedSpec;
    }

    @Override
    public void save(final NodeSettingsWO settings) {
        m_delegate.save(settings);
    }

    @Override
    public ValueFactory<?, ?>[] getValueFactories() {
        return m_delegate.getValueFactories();
    }

    @Override
    public Iterator<DataSpec> iterator() {
        return m_delegate.iterator();
    }

    @Override
    public int hashCode() {
        return m_delegate.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return m_delegate.equals(obj);
    }
    
    public DataTraits getTraits(final int index) {
        return m_delegate.getTraits(index);
    }

}
