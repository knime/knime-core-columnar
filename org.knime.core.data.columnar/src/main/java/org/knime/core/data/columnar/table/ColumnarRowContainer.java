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
 *   Nov 9, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;

import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExtensionTable;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.WriteAccessRow;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class ColumnarRowContainer implements RowContainer, RowWriteAccessible {

    @SuppressWarnings("resource") // Columnar table will be closed along with row container.
    static ColumnarRowContainer create(final ExecutionContext context, final int id, final ValueSchema schema,
        final ColumnStoreFactory storeFactory, final ColumnarRowWriteTableSettings settings) throws IOException {
        // TODO: turn this into a constructor?
        return new ColumnarRowContainer(context, id, new ColumnarRowWriteTable(schema, storeFactory, settings));
    }

    private final ExecutionContext m_context;

    private final int m_id;

    private final ColumnarRowWriteTable m_columnarTable;

    private ExtensionTable m_finishedTable;

    private ColumnarRowContainer(final ExecutionContext context, final int id,
        final ColumnarRowWriteTable columnarTable) {
        m_context = context;
        m_id = id;
        m_columnarTable = columnarTable;
    }

    @Override
    public ValueSchema getSchema() {
        return m_columnarTable.getSchema();
    }

    @Override
    public ColumnarRowWriteCursor createCursor() {
        return m_columnarTable.createCursor();
    }

    void flushObjects() throws IOException {
        m_columnarTable.flushObjects();
    }

    @SuppressWarnings("resource")
    @Override
    public BufferedDataTable finish() {
        if (m_context == null) {
            throw new IllegalStateException(
                "ColumnarRowContainer has not been initialized with ExecutionContext. Implementation error.");
        }
        return finishInternal().create(m_context);
    }

    @Override
    public final void close() {
        m_columnarTable.close();
    }

    ExtensionTable finishInternal() {
        if (m_finishedTable == null) {
            @SuppressWarnings("resource") // Will be closed along with the container table.
            final ColumnarRowReadTable finishedColumnarTable = m_columnarTable.finish();
            m_finishedTable =
                UnsavedColumnarContainerTable.create(m_id, finishedColumnarTable, m_columnarTable.getStoreFlusher());
        }
        return m_finishedTable;
    }

    /**
     * Only to be used by ColumnarDataContainerDelegate#setMaxPossibleValues(int) for backward compatibility reasons.
     *
     * @param maxPossibleValues the maximum number of values for a nominal domain.
     *
     * @apiNote No API.
     */
    void setMaxPossibleValues(final int maxPossibleValues) {
        m_columnarTable.setMaxPossibleValues(maxPossibleValues);
    }


    // -- implement RowWriteAccessible --

    @Override
    public WriteCursor<WriteAccessRow> getWriteCursor() {
        return m_columnarTable.getWriteCursor();
    }
}
