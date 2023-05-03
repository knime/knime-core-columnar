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

import java.io.Flushable;
import java.io.IOException;

import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.virtual.WriteAccessRowWrite;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.WriteAccessRow;

/**
 * Columnar implementation of {@link RowWriteCursor} for writing data to a columnar table backend.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarRowWriteCursor implements RowWriteCursor {

    // the maximum capacity (in number of held elements) of a single chunk
    // subtract 750 since arrow rounds up to the next power of 2 anyways
    static final int CAPACITY_MAX_DEF = (1 << 15) - 750; // 32,018

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarRowWriteCursor.class);

    private final WriteCursor<WriteAccessRow> m_accessCursor;

    private final RowWrite m_rowWrite;

    private long m_size = 0;

    private final Flushable m_flushOnForward;

    /**
     * Create a row write cursor
     * @param store The batch store to write to
     * @param factories Value factories for the individual columns
     * @param flushOnForward An optional {@link Flushable} that will be flushed on each forward operation
     */
    ColumnarRowWriteCursor(final BatchStore store, final ColumnarValueSchema schema, final Flushable flushOnForward) {

        m_accessCursor = ColumnarWriteCursorFactory.createWriteCursor(store);
        m_flushOnForward = flushOnForward;
        m_rowWrite = new WriteAccessRowWrite(schema, m_accessCursor.access());
    }

    @Override
    public final RowWrite forward() {
        if (m_flushOnForward != null) {
            try {
                m_flushOnForward.flush();
            } catch (IOException ex) {
                LOGGER.error("Could not flush cursor during forward", ex);
            }
        }
        m_size++;
        return m_accessCursor.forward() ? m_rowWrite : null;
    }

    @Override
    public boolean canForward() {
        return true;
    }

    @Override
    public final void close() {
        try {
            m_accessCursor.close();
        } catch (IOException ex) {
            // This exception is usually not critical, since we are done with m_accessCursor.
            // It could be a ClosedByInterruptException as a consequence of the thread being interrupted on node cancel.
            LOGGER.warn("Closing the write access cursor failed.", ex);
        }
    }

    long size() {
        return m_size;
    }

    /**
     * Make sure the current contents of this {@link ColumnarRowWriteCursor} have been
     * written to disk. Blocks until this is true. Does not close this cursor.
     */
    public void flush() {
        try {
            m_accessCursor.flush();
        } catch (IOException ex) {
            // This exception is usually not critical, similar to #close()
            LOGGER.warn("Finishing writing failed because flushing the write access cursor failed.", ex);
        }
    }

    int getNumColumns() {
        return m_rowWrite.getNumColumns();
    }

    /**
     * @return the accessCursor
     */
    WriteCursor<WriteAccessRow> getAccessCursor() {
        return m_accessCursor;
    }
}
