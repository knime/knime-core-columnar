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
 *   Apr 13, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cursor;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarReadAccess;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;

/**
 * Defines and creates {@code ColumnarReadAccessRows}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public /* TODO (TP) remove public modifier (again) */ final class ColumnarReadAccessRowFactory {

    private final ColumnarSchema m_schema;

    public /* TODO (TP) remove public modifier (again) */ ColumnarReadAccessRowFactory(final ColumnarSchema schema) {
        m_schema = schema;
    }

    public /* TODO (TP) remove public modifier (again) */ interface ColumnarReadAccessRow extends ReadAccessRow {

        void setBatch(ReadBatch batch);
    }

    public /* TODO (TP) remove public modifier (again) */ ColumnarReadAccessRow createRow(final ColumnDataIndex indexInBatch, final ColumnSelection columnSelection) {
        if (columnSelection.allSelected()) {
            return new FullColumnarReadAccessRow(m_schema.specStream()//
                .map(s -> createReadAccess(s, indexInBatch))//
                .toArray(ColumnarReadAccess[]::new));
        }
        var selected = columnSelection.getSelected();
        if (selected.length == 1) {
            int selectedColumn = selected[0];
            return new SingleColumnReadAccessRow(m_schema.numColumns(), selectedColumn,
                createReadAccess(m_schema.getSpec(selectedColumn), indexInBatch));
        }
        return new SelectedColumnarReadAccessRow(m_schema, columnSelection, indexInBatch);
    }

    private static ColumnarReadAccess createReadAccess(final DataSpec spec, final ColumnDataIndex index) {
        return ColumnarAccessFactoryMapper.createAccessFactory(spec).createReadAccess(index);
    }

    private static final class FullColumnarReadAccessRow implements ColumnarReadAccessRow {

        private final ColumnarReadAccess[] m_accesses;

        FullColumnarReadAccessRow(final ColumnarReadAccess[] accesses) {
            m_accesses = accesses;
        }

        @Override
        public int size() {
            return m_accesses.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            return (A)m_accesses[index];
        }

        @Override
        public void setBatch(final ReadBatch batch) {
            for (int i = 0; i < m_accesses.length; i++) {
                m_accesses[i].setData(batch.get(i));
            }
        }

    }

    private static final class SelectedColumnarReadAccessRow implements ColumnarReadAccessRow {

        private final int[] m_selectedIndices;

        private final ColumnarReadAccess[] m_accesses;

        SelectedColumnarReadAccessRow(final ColumnarSchema schema, final ColumnSelection selection,
            final ColumnDataIndex index) {
            m_selectedIndices = selection.getSelected();
            m_accesses = new ColumnarReadAccess[schema.numColumns()];
            for (int i : m_selectedIndices) {
                m_accesses[i] =
                    ColumnarAccessFactoryMapper.createAccessFactory(schema.getSpec(i)).createReadAccess(index);
            }
        }

        @Override
        public int size() {
            return m_accesses.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            return (A)m_accesses[index];
        }

        @Override
        public void setBatch(final ReadBatch batch) {
            for (int i : m_selectedIndices) {
                m_accesses[i].setData(batch.get(i));
            }
        }

    }

    private static final class SingleColumnReadAccessRow implements ColumnarReadAccessRow {

        private final int m_numColumns;

        private final int m_selectedColumn;

        private final ColumnarReadAccess m_access;

        SingleColumnReadAccessRow(final int numColumns, final int selectedColumn, final ColumnarReadAccess access) {
            m_numColumns = numColumns;
            m_selectedColumn = selectedColumn;
            m_access = access;
        }

        @Override
        public int size() {
            return m_numColumns;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            return index == m_selectedColumn ? (A)m_access : null;
        }

        @Override
        public void setBatch(final ReadBatch batch) {
            m_access.setData(batch.get(m_selectedColumn));
        }

    }


}
