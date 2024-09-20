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

import org.knime.core.columnar.cursor.ColumnarCursorFactory;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.data.columnar.filter.TableFilterUtils;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.ReadAccessRowRead;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;

/**
 * Columnar implementations of {@link RowCursor} for reading data from columnar table backend.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class ColumnarRowCursorFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarRowCursorFactory.class);

    private static final class EmptyRowCursor implements RowCursor {

        private final ValueSchema m_schema;

        private EmptyRowCursor(final ValueSchema schema) {
            m_schema = schema;
        }

        @Override
        public RowRead forward() {
            return null;
        }

        @Override
        public boolean canForward() {
            return false;
        }

        @Override
        public void close() {
            // this cursor holds no resources
        }

        @Override
        public int getNumColumns() {
            return m_schema.numColumns() - 1;
        }

    }

    private static final class DefaultRowCursor implements RowCursor {

        private final LookaheadCursor<ReadAccessRow> m_delegate;

        private final RowRead m_rowRead;

        private DefaultRowCursor(final LookaheadCursor<ReadAccessRow> delegate, final ValueSchema schema) {
            m_delegate = delegate;
            m_rowRead = new ReadAccessRowRead(schema, delegate.access());
        }

        @Override
        public boolean canForward() {
            return m_delegate.canForward();
        }

        @Override
        public RowRead forward() {
            return m_delegate.forward() ? m_rowRead : null;
        }

        @Override
        public int getNumColumns() {
            return m_rowRead.getNumColumns();
        }

        @Override
        public void close() {
            try {
                m_delegate.close();
            } catch (IOException ex) {
                final String error = "Exception while closing batch reader.";
                LOGGER.error(error, ex);
                throw new IllegalStateException(error, ex);
            }
        }

    }

    static RowCursor create(final BatchReadStore store, final ValueSchema schema, final long size) {
        return create(store, schema, size, null);
    }

    @SuppressWarnings("resource") // the returned cursor has to be closed by the caller
    static RowCursor create(final BatchReadStore store, final ValueSchema schema, final long size, // NOSONAR
        final TableFilter filter) {
        if (size < 1) {
            return new EmptyRowCursor(schema);
        }

        var selection = filter == null ? Selection.all() : TableFilterUtils.toSelection(filter, size);
        return new DefaultRowCursor(ColumnarCursorFactory.create(store, selection), schema);
    }

    private ColumnarRowCursorFactory() {
    }

}
