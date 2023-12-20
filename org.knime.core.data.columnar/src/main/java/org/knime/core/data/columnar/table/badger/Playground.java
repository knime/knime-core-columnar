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
 *   20 Dec 2023 (pietzsch): created
 */
package org.knime.core.data.columnar.table.badger;

import java.io.IOException;
import java.util.Arrays;

import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory.ColumnarWriteCursor;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccessRow;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 *
 * @author pietzsch
 */
public class Playground {



    private final MyCursor m_writeCursor;

    public Playground(final BatchStore store)
    {
        ColumnarSchema schema = store.getSchema();

        m_writeCursor = new MyCursor(schema, 1000);


    }


    static class Badger {

        private final MyCursor m_writeCursor;
        private final BatchStore m_store;

        Badger(final MyCursor cursor, final BatchStore store)
        {
            m_writeCursor = cursor;
            m_store = store;
            m_current_batch = m_store.getWriter().create(cursor.m_capacity);
        }

        private int m_previous_index = 0;
        private WriteBatch m_current_batch;

        void tryAdvance()
        {
            int index = m_writeCursor.m_current;
            for ( int i = m_previous_index; i < index; ++i )
            {
                int numCols = m_current_batch.numData();
                for( int col = 0; col < numCols; ++col ) {
                    ReadAccess access = m_writeCursor.m_access.getAccess(col);
                    NullableWriteData d = m_current_batch.get(col);
                }

            }
        }
    }


    static class MyCursor implements ColumnarWriteCursor
    {
        private final BufferedAccessRow[] m_buffers;

        private final BufferedAccessRow m_access;

        private final int m_capacity;

        MyCursor( final ColumnarSchema schema, final int capacity)
        {
            m_capacity = capacity;
            m_buffers = new BufferedAccessRow[ capacity ];
            Arrays.setAll(m_buffers, i -> BufferedAccesses.createBufferedAccessRow(schema));
            m_access = BufferedAccesses.createBufferedAccessRow(schema);
        }

        @Override
        public WriteAccessRow access() {
            return m_access;
        }

        private int m_current = -1;

        private long m_offset = 1;

        @Override
        public boolean forward() {
            if (m_current >= 0) {
                m_buffers[m_current].setFrom(m_access);
            }
            if(++m_current == m_capacity) {
                m_current = 0;
                m_offset += m_capacity;
            }
            return true;
        }

        @Override
        public void flush() throws IOException {
            // TODO Auto-generated method stub
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub
        }

        @Override
        public long getNumForwards() {
            return m_offset + m_current;
        }
    }


}
