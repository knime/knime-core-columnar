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
 *   Jul 28, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.util.stream.IntStream;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.v2.RowKeyWriteValue;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.table.row.Selection.ColumnSelection;
import org.knime.core.table.row.WriteAccessRow;

/**
 * Implements a {@link RowWrite} based on a {@link WriteAccessRow}. Also allows to directly {@link DataRow DataRows} in
 * order to avoid the overhead of wrapping the {@link DataRow} into a {@link RowRead} first.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class WriteAccessRowWrite implements RowWrite {

    // TODO with a bit of refactoring this could also be used in BufferedRowContainer

    private final WriteAccessRow m_accesses;

    private final WriteValue<?>[] m_values;

    private final RowKeyWriteValue m_rowKeyValue;

    /**
     * Constructor.
     *
     * @param schema of the table
     * @param writeAccess to write to
     * @param selection the selected columns
     */
    public WriteAccessRowWrite(final ColumnarValueSchema schema, final WriteAccessRow writeAccess,
        final ColumnSelection selection) {
        m_accesses = writeAccess;
        final var numColumns = schema.numColumns();
        m_values = new WriteValue<?>[numColumns];
        var selected = selection.allSelected() ? IntStream.range(0, numColumns)
            : IntStream.of(selection.getSelected(0, numColumns));
        selected.forEach(i -> m_values[i] = schema.getValueFactory(i).createWriteValue(m_accesses.getWriteAccess(i)));
        m_rowKeyValue = (RowKeyWriteValue)m_values[0];
    }

    /**
     * Constructor.
     *
     * @param schema of the table
     * @param writeAccess to write to
     */
    public WriteAccessRowWrite(final ColumnarValueSchema schema, final WriteAccessRow writeAccess) {
        this(schema, writeAccess, ColumnSelection.all());
    }


    @Override
    public void setFrom(final RowRead values) {
        // TODO introduce ColumnarRowRead that provides the accesses/ReadAccessRow
        if (values instanceof DenseColumnarRowRead columnarRowRead) {
            var readAccesses = columnarRowRead.getAccesses();
            for (int i = 0; i < m_accesses.size(); i++) {
                m_accesses.getWriteAccess(i).setFrom(readAccesses[i]);
            }
            return;
        }

        assert values.getNumColumns() == getNumColumns();
        setRowKey(values.getRowKey());
        for (var i = 0; i < values.getNumColumns(); i++) {
            if (values.isMissing(i)) {
                setMissing(i);
            } else {
                final var writeValue = m_values[i + 1];
                if (writeValue != null) {
                    writeValue.setValue(values.getValue(i));
                }
            }
        }
    }

    /**
     * Copy all values as well as the row key from the provided row.
     *
     * @param row to read from
     */
    public void setFrom(final DataRow row) {
        assert row.getNumCells() == getNumColumns();
        setRowKey(row.getKey());
        for (var i = 0; i < row.getNumCells(); i++) {
            final DataCell cell = row.getCell(i);
            if (cell.isMissing()) {
                setMissing(i);
            } else {
                var writeValue = m_values[i + 1];
                if (writeValue != null) {
                    setValue(writeValue, cell);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <V extends DataValue> void setValue(final WriteValue<V> writeValue, final DataValue value) {
        writeValue.setValue((V)value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <W extends WriteValue<?>> W getWriteValue(final int index) {
        return (W)m_values[index + 1];
    }

    @Override
    public int getNumColumns() {
        return m_values.length - 1;
    }

    @Override
    public void setMissing(final int index) {
        if (m_values[index + 1] != null) {
            m_accesses.getWriteAccess(index + 1).setMissing();
        }
    }

    @Override
    public void setRowKey(final String rowKey) {
        if (m_rowKeyValue != null) {
            m_rowKeyValue.setRowKey(rowKey);
        }
    }

    @Override
    public void setRowKey(final RowKeyValue rowKey) {
        if (m_rowKeyValue != null) {
            m_rowKeyValue.setRowKey(rowKey);
        }
    }

}