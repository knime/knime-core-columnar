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
package org.knime.core.columnar.badger;

import java.io.IOException;
import java.util.Arrays;

import org.knime.core.columnar.access.ColumnarAccessFactory;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cursor.ColumnarWriteCursorFactory.ColumnarWriteCursor;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccessRow;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 *
 * @author pietzsch
 */
public class HeapBadger {

    // TODO we should make this depend on the size of the data that we know about in advance
    /** max number of rows in one batch */
    private static final int DEFAULT_MAX_NUM_ROWS_PER_BATCH = 500;

    /** max size of a batch */
    private static final int DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 5000;

    private final BadgerWriteCursor m_writeCursor;

    private final Badger m_badger;

    private final int m_maxNumRowsPerBatch;

    private final int m_maxBatchSizeInBytes;

    public HeapBadger(final BatchStore store, final int maxNumRowsPerBatch, final int maxBatchSizeInBytes) {
        ColumnarSchema schema = store.getSchema();
        m_maxNumRowsPerBatch = maxNumRowsPerBatch;
        m_maxBatchSizeInBytes = maxBatchSizeInBytes;

        m_writeCursor = new BadgerWriteCursor(schema, 20);
        m_badger = new Badger(store);
    }

    public HeapBadger(final BatchStore store) {
        this(store, DEFAULT_MAX_NUM_ROWS_PER_BATCH, DEFAULT_MAX_BATCH_SIZE_IN_BYTES);
    }

    public ColumnarWriteCursor getWriteCursor() {
        return m_writeCursor;
    }

    class Badger {

        private final BatchWriter m_writer;

        private final BatchStore m_store;

        Badger(final BatchStore store) {
            m_store = store;
            m_writer = m_store.getWriter();
            m_current_batch = null;

            final ColumnarSchema schema = m_store.getSchema();
            final int numColumns = schema.numColumns();
            m_accessesToTheCurrentBatchs = new ColumnarWriteAccess[numColumns];
            for (int c = 0; c < numColumns; ++c) {
                ColumnarAccessFactory factory = ColumnarAccessFactoryMapper.createAccessFactory(schema.getSpec(c));
                m_accessesToTheCurrentBatchs[c] = factory.createWriteAccess(() -> m_index_in_writebatch);
            }

            switchToNextBatch();
        }

        private final ColumnarWriteAccess[] m_accessesToTheCurrentBatchs;

        private int m_index_in_writebatch = 0;

        private int m_previous_head = 0;

        private WriteBatch m_current_batch;

        private void writeBufferedRow(final int row) {
            final BufferedAccessRow bufferedRow = m_writeCursor.m_buffers[row];

            // Set the data from the buffer
            for (int col = 0; col < m_accessesToTheCurrentBatchs.length; ++col) {
                m_accessesToTheCurrentBatchs[col].setFrom(bufferedRow.getAccess(col));
            }
            ++m_index_in_writebatch;

            // TODO use the size tracker by Adrian
            if (m_current_batch.sizeOf() >= m_maxBatchSizeInBytes) {
                System.out.println("NEW BATCH!");
                try {
                    // TODO if we have written more data in some columns make sure we do not loose it
                    writeCurrentBatch();
                    switchToNextBatch();
                } catch (IOException ex) {
                    // TODO: handle exception
                }
            }
        }

        /**
         * Write buffered rows to the underlying store. Split batches when they become large enough.
         *
         * @return up to which index buffered data has been serialized.
         */
        int tryAdvance() {

            final int head = m_writeCursor.m_current;

            System.out.println("tryAdvance()");
            System.out.println(" head = " + head);
            System.out.println(" m_previous_head = " + m_previous_head);


//          [0,1,2,3,4,5]
//           |     ^
//           0,1,2,
//                 |

//            weil m_current > previous_head
//            darf write_cursor bis zum ende schreiben, wrappen, und bis m_current-1 schreiben

//          [0,1,2,3,4,5]
//             ^     |
//           0,      4,5
//             |

//          weil m_current < previous_head
//          darf write_cursor nur bis m_current-1 schreiben


            final int you_may_write_to = m_previous_head;

            final boolean wrap = head < m_previous_head;
            if (wrap) {
                System.out.println(" wrap");
                final int len = m_writeCursor.m_bufferSize;
                for (int i = m_previous_head; i < len; ++i) {
                    System.out.println( " writeBufferedRow("+i+")");
                    writeBufferedRow(i);
                }
                for (int i = 0; i < head; ++i) {
                    System.out.println( " writeBufferedRow("+i+")");
                    writeBufferedRow(i);
                }
            } else {
                System.out.println(" !wrap");
                for (int i = m_previous_head; i < head; ++i) {
                    System.out.println( " writeBufferedRow("+i+")");
                    writeBufferedRow(i);
                }
            }
            m_previous_head = head;

            return you_may_write_to - 1; // TODO -1? really?
        }

        void finish() throws IOException {
            tryAdvance();
            writeCurrentBatch();
        }

        private void writeCurrentBatch() throws IOException {
            ReadBatch readBatch = m_current_batch.close(m_index_in_writebatch);
            m_writer.write(readBatch);
            readBatch.release();
        }

        private void switchToNextBatch() {
            // Create the next batch
            m_current_batch = m_writer.create(m_maxNumRowsPerBatch);

            // Connect the accesses with the current write batch
            for (int col = 0; col < m_accessesToTheCurrentBatchs.length; col++) {
                m_accessesToTheCurrentBatchs[col].setData(m_current_batch.get(col));
            }

            m_index_in_writebatch = 0;
        }
    }

    class BadgerWriteCursor implements ColumnarWriteCursor {
        private final BufferedAccessRow[] m_buffers;

        private final BufferedAccessRow m_access;

        private final int m_bufferSize;

        BadgerWriteCursor(final ColumnarSchema schema, final int bufferSize) {
            m_bufferSize = bufferSize;
            m_buffers = new BufferedAccessRow[bufferSize];
            Arrays.setAll(m_buffers, i -> BufferedAccesses.createBufferedAccessRow(schema));
            m_access = BufferedAccesses.createBufferedAccessRow(schema);
        }

        @Override
        public WriteAccessRow access() {
            return m_access;
        }

        private int m_current = -1;

        private int m_bound = -1;

        private long m_offset = 1;

        @Override
        public boolean forward() {
            only_forward();

            // synchonously write
            if ( getNumForwards() % 7 == 0) {
                m_bound = m_badger.tryAdvance();
            }

            return true;
        }

        private boolean only_forward() {
            System.out.println("only_forward()");

            if (m_current >= 0) {
                m_buffers[m_current].setFrom(m_access);
            }
            m_current++;

//            if ( m_current == m_bound ) {
//                // TODO: block until buffered data has been serialized
//                while (m_current == m_bound) {
//                    m_bound = m_badger.tryAdvance();
//                }
//            }
//          weil m_current > previous_head
//          darf write_cursor bis zum ende schreiben, wrappen, und bis m_current-1 schreiben
//          weil m_current < previous_head
//          darf write_cursor nur bis m_current-1 schreiben


            // Ring buffer
            if (m_current == m_bufferSize) {
                // TODO: when writing asynchronously, we have to make sure that we don't overwrite old
                // buffers that haven't been serialized yet
                m_current = 0;
                m_offset += m_bufferSize;
                System.out.println(" wrapped!");
            }

            System.out.println(" m_current = " + m_current);
            System.out.println(" getNumForwards() = " + getNumForwards());

            return true;
        }

        // TODO rename to finish() ???
        @Override
        public void flush() throws IOException {
            only_forward();
            m_badger.finish();
        }

        @Override
        public void close() throws IOException {
            // TODO abort, release resources
        }

        @Override
        public long getNumForwards() {
            return m_offset + m_current;
        }
    }

}
