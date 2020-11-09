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

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.v2.Cursor;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.DuplicateKeyException;

final class ColumnarDataContainerDelegate implements DataContainerDelegate {

    private final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarDataContainerDelegate.class);

    private final Cursor<RowWrite> m_delegateCursor;

    private final DataTableSpec m_spec;

    private final int m_numColumns;

    private final ColumnarRowContainer m_delegateContainer;

    private long m_size;

    private ContainerTable m_containerTable;

    ColumnarDataContainerDelegate(final DataTableSpec spec, final ColumnarRowContainer container) {
        m_delegateContainer = container;
        m_delegateCursor = container.createCursor();
        m_spec = spec;
        m_numColumns = m_spec.getNumColumns();
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
        m_delegateContainer.setMaxPossibleValues(maxPossibleValues);
    }

    @Override
    public void addRowToTable(final DataRow row) {
        if (row.getNumCells() != m_numColumns) {
            throw new IllegalStateException(
                "Cell count in row " + row.getKey().toString() + " is not equal to length of column names array: "
                    + row.getNumCells() + " vs. " + m_spec.getNumColumns());
        }

        // TODO we could avoid this method call for cases where we know RowWrite is always the same
        final RowWrite rowWrite = m_delegateCursor.forward();

        // TODO in case of no key, this method call is not required.
        rowWrite.setRowKey(row.getKey());
        for (int i = 0; i < m_numColumns; i++) {
            final DataCell cell = row.getCell(i);
            if (!cell.isMissing()) {
                rowWrite.<WriteValue<DataCell>> getWriteValue(i).setValue(cell);
            } else {
                rowWrite.setMissing(i);
            }
        }
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
            m_containerTable = m_delegateContainer.finishInternal();
            m_delegateCursor.close();
        } catch (DuplicateKeyException ex) {
            throw ex;
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void clear() {
        if (m_containerTable != null) {
            m_containerTable.clear();
        } else {
            try {
                m_delegateCursor.close();
                m_delegateContainer.close();
            } catch (Exception e) {
                // NB: best effort for clearing
                LOGGER.debug("Exception while clearing ColumnarDataContainer. ", e);
            }
        }
    }
}
