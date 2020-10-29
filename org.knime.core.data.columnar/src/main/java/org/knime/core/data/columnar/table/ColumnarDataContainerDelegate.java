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
package org.knime.core.data.columnar.table;

import java.io.IOException;

import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.value.CustomRowKeyValueFactory.CustomRowKeyWriteValue;

/**
 * Columnar implementation of {@link DataContainerDelegate}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class ColumnarDataContainerDelegate implements DataContainerDelegate {

    private final ColumnarRowWriteCursor m_delegate;

    private final DataTableSpec m_spec;

    private final boolean m_isCustomRowKey;

    private final int m_numColumns;

    private long m_size;

    private ContainerTable m_containerTable;

    /**
     * @param cursor the delegate. writes into a {@link ColumnStore}.
     * @throws IOException
     */
    public ColumnarDataContainerDelegate(final ColumnarRowWriteCursor cursor) throws IOException {
        m_delegate = cursor;
        m_spec = cursor.getSchema().getSourceSpec();
        m_numColumns = m_spec.getNumColumns();
        m_isCustomRowKey = cursor.getSchema().getRowKeyType() == RowKeyType.CUSTOM;
    }

    @Override
    public DataTableSpec getTableSpec() {
        if (m_containerTable != null) {
            return m_containerTable.getDataTableSpec();
        }
        return m_spec;
    }

    @Override
    public long size() {
        return m_size;
    }

    @Deprecated
    @Override
    public void setMaxPossibleValues(final int maxPossibleValues) {
        m_delegate.setMaxPossibleValues(maxPossibleValues);
    }

    @Override
    public void addRowToTable(final DataRow row) {
        if (row.getNumCells() != m_numColumns) {
            throw new IllegalStateException(
                "Cell count in row " + row.getKey().toString() + " is not equal to length of column names array: "
                    + row.getNumCells() + " vs. " + m_spec.getNumColumns());
        }

        if (m_isCustomRowKey) {
            m_delegate.<CustomRowKeyWriteValue> getWriteValue(-1).setRowKey(row.getKey());
        }

        for (int i = 0; i < m_numColumns; i++) {
            final DataCell cell = row.getCell(i);
            if (!cell.isMissing()) {
                m_delegate.<WriteValue<DataCell>> getWriteValue(i).setValue(cell);
            } else {
                m_delegate.setMissing(i);
            }
        }
        m_delegate.push();
        m_size++;
    }

    @Override
    public ContainerTable getTable() {
        if (m_containerTable == null) {
            throw new IllegalStateException("getTable() can only be called after close() was called.");
        }
        return m_containerTable;
    }

    @Override
    public void close() {
        try {
            m_containerTable = m_delegate.finish();
            m_delegate.close();
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void clear() {
        if (m_containerTable != null) {
            m_containerTable.clear();
        } else {
            try {
                m_delegate.close();
            } catch (final Exception ex) {
                throw new IllegalStateException("Exception thrown while cleaning delegate table.", ex);
            }
        }
    }
}
