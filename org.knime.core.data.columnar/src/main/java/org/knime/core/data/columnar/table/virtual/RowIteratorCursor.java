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
 *   Jul 20, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.io.IOException;
import java.util.Arrays;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataValue;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.v2.RowKeyWriteValue;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccessRow;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection.ColumnSelection;

/**
 * A {@link Cursor} that is backed by a {@link CloseableRowIterator}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class RowIteratorCursor implements LookaheadCursor<ReadAccessRow> {

    private final BufferedAccessRow m_accesses;

    private final CloseableRowIterator m_iterator;

    private final RowKeyWriteValue m_rowKeyValue;

    private final WriteValue<?>[] m_values;

    RowIteratorCursor(final ValueSchema schema, final CloseableRowIterator iterator,
        final ColumnSelection selection) {
        m_accesses = BufferedAccesses.createBufferedAccessRow(schema, selection);
        m_values = new WriteValue<?>[schema.numColumns()];
        Arrays.setAll(m_values, i -> selection.isSelected(i) //
            ? schema.getValueFactory(i).createWriteValue(m_accesses.getWriteAccess(i)) //
            : null);
        m_rowKeyValue = (RowKeyWriteValue)m_values[0];
        m_iterator = iterator;
    }

    RowIteratorCursor(final ValueSchema schema, final CloseableRowIterator iterator) {
        this(schema, iterator, ColumnSelection.all());
    }

    @Override
    public void close() throws IOException {
        m_iterator.close();
    }

    @Override
    public ReadAccessRow access() {
        return m_accesses;
    }

    @Override
    public boolean forward() {
        if (m_iterator.hasNext()) {
            // Note: all cells of a BufferedAccessRow need to be populated when advancing to a new row,
            //       which we do here, so no need to call setMissing() on all cells first.
            final DataRow row = m_iterator.next();
            assert row.getNumCells() == m_values.length - 1;
            if (m_rowKeyValue != null) {
                m_rowKeyValue.setRowKey(row.getKey());
            }
            for (int i = 0; i < m_values.length - 1; i++) {
                WriteValue<?> writeValue = m_values[i + 1];
                if (writeValue != null) {
                    final DataCell cell = row.getCell(i);
                    if (cell.isMissing()) {
                        m_accesses.getWriteAccess(i + 1).setMissing();
                    } else {
                        set(writeValue, cell);
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static <D extends DataValue> void set(final WriteValue<D> writeValue, final DataCell cell) {
        writeValue.setValue((D)cell);
    }

    @Override
    public boolean canForward() {
        return m_iterator.hasNext();
    }
}
