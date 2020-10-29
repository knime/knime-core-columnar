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

import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueSchema;
import org.knime.core.data.v2.access.ReadAccess;
import org.knime.core.data.v2.access.WriteAccess;

final class DefaultColumnarValueSchema implements ColumnarValueSchema {

    private final ValueSchema m_source;

    private final ColumnarAccessFactory<ColumnReadData, ReadAccess, ColumnWriteData, WriteAccess>[] m_factories;

    public DefaultColumnarValueSchema(final ValueSchema source, final ColumnarAccessFactory<?, ?, ?, ?>[] factories) {
        m_source = source;
        @SuppressWarnings("unchecked")
        final ColumnarAccessFactory<ColumnReadData, ReadAccess, ColumnWriteData, WriteAccess>[] cast =
            (ColumnarAccessFactory<ColumnReadData, ReadAccess, ColumnWriteData, WriteAccess>[])factories;
        m_factories = cast;
    }

    @Override
    public ColumnDataSpec getColumnDataSpec(final int index) {
        return m_factories[index].getColumnDataSpec();
    }

    @Override
    public <C extends ColumnWriteData> ColumnarWriteValueFactory<C> getWriteValueFactoryAt(final int index) {
        @SuppressWarnings("unchecked")
        final ColumnarAccessFactory<ColumnReadData, ReadAccess, C, WriteAccess> factory =
            (ColumnarAccessFactory<ColumnReadData, ReadAccess, C, WriteAccess>)m_factories[index];
        return new DefaultWriteValueFactory<>(factory, m_source.getFactoryAt(index));
    }

    @Override
    public ColumnarReadValueFactory<ColumnReadData> getReadValueFactoryAt(final int index) {
        return new DefaultReadValueFactory<ColumnReadData, ReadAccess>(m_factories[index],
            m_source.getFactoryAt(index));
    }

    @Override
    public int getNumColumns() {
        return m_source.getNumColumns();
    }

    @Override
    public DataTableSpec getSourceSpec() {
        return m_source.getSourceSpec();
    }

    @Override
    public ValueSchema getSourceSchema() {
        return m_source;
    }

    @Override
    public RowKeyType getRowKeyType() {
        return m_source.getRowKeyType();
    }
}