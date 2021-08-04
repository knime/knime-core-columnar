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
 *   Jul 26, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowKeyReadValue;
import org.knime.core.data.v2.RowRead;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.row.ReadAccessRow;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

final class FilteredRowRead implements RowRead {

    private final ColumnarValueSchema m_schema;

    private final TIntObjectMap<ReadValue> m_values;

    private final TIntObjectMap<ReadAccess> m_accesses;

    private final RowKeyReadValue m_rowKey;

    FilteredRowRead(final ColumnarValueSchema unfilteredSchema, final ReadAccessRow filteredReadAccessRow,
        final ColumnSelection selectedColumns) {
        m_schema = unfilteredSchema;
        m_values = new TIntObjectHashMap<>();
        m_accesses = new TIntObjectHashMap<>();
        // TODO introduce method to efficiently iterate over selected indices
        var unfilteredIndices = 0;
        for (var i = 0; i < selectedColumns.numColumns(); i++) {
            if (selectedColumns.isSelected(i)) {
                final var access = filteredReadAccessRow.getAccess(unfilteredIndices);
                m_accesses.put(i, access);
                m_values.put(i, unfilteredSchema.getValueFactory(i).createReadValue(access));
                unfilteredIndices++;
            }
        }
        m_rowKey = (RowKeyReadValue)m_values.get(0);
    }

    @Override
    public int getNumColumns() {
        return m_schema.numColumns() - 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <D extends DataValue> D getValue(final int index) {
        // TODO what if the index is filtered?
        return (D)m_values.get(index + 1);
    }

    @Override
    public boolean isMissing(final int index) {
        // TODO what if the index is filtered?
        return m_accesses.get(index + 1).isMissing();
    }

    @Override
    public RowKeyValue getRowKey() {
        return m_rowKey;
    }

}