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
package org.knime.core.data.columnar.schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.traits.DataTraits;

import com.google.common.collect.Iterators;

/**
 * Default implementation of a {@link ColumnarValueSchema}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
final class DefaultColumnarValueSchema implements ColumnarValueSchema {

    private final ValueSchema m_source;

    private final List<DataSpec> m_specs;

    private final List<DataTraits> m_traits;

    DefaultColumnarValueSchema(final ValueSchema source) {
        m_source = source;
        final int numFactories = source.numFactories();
        m_specs = new ArrayList<>(numFactories);
        m_traits = new ArrayList<>(numFactories);
        for (int i = 0; i < numFactories; i++) {//NOSONAR
            final var factory = source.getValueFactory(i);
            m_specs.add(factory.getSpec());
            m_traits.add(ValueFactoryUtils.getTraits(factory));
        }
    }

    ValueSchema getSource() {
        return m_source;
    }

    @Override
    public DataSpec getSpec(final int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.format("Column index %d smaller than 0.", index));
        }
        if (index >= numColumns()) {
            throw new IndexOutOfBoundsException(
                String.format("Column index %d greater or equal to the reader's largest column index (%d).", index,
                    numColumns() - 1));
        }

        return m_specs.get(index);
    }

    @Override
    public DataTableSpec getSourceSpec() {
        return m_source.getSourceSpec();
    }

    @Override
    public void save(final NodeSettingsWO settings) {
        ValueSchemaUtils.save(m_source, settings);
    }

    @Override
    public int numColumns() {
        return m_specs.size();
    }

    @Override
    public Iterator<DataSpec> iterator() {
        return m_specs.iterator();
    }

    @Override
    public int hashCode() {
        return m_specs.hashCode();
    }

    @Override
    public String toString() {
        return "Columns (" + m_specs.size() + ") "
            + StringUtils.join(m_specs.stream().map(Object::toString).iterator(), ",");
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ColumnarSchema)) { // NOSONAR
            return false;
        }
        final ColumnarSchema other = (ColumnarSchema)obj;
        if (numColumns() != other.numColumns()) {
            return false;
        }
        return Iterators.elementsEqual(iterator(), other.iterator());
    }

    @Override
    public DataTraits getTraits(final int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.format("Column index %d smaller than 0.", index));
        }
        if (index >= numColumns()) {
            throw new IndexOutOfBoundsException(
                String.format("Column index %d greater or equal to the reader's largest column index (%d).", index,
                    numColumns() - 1));
        }

        return m_traits.get(index);
    }

    @Override
    public <R extends ReadAccess, W extends WriteAccess> ValueFactory<R, W> getValueFactory(final int index) {
        return m_source.getValueFactory(index);
    }

    @Override
    public Stream<DataSpec> specStream() {
        return m_specs.stream();
    }

}
