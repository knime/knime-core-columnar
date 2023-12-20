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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.batch.BatchWriter;
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

    private final BadgerWriteCursor m_writeCursor;

    private final Badger m_badger;

    public HeapBadger(final BatchStore store) {
        ColumnarSchema schema = store.getSchema();

        m_writeCursor = new BadgerWriteCursor(schema, 1000);
        m_badger = new Badger(m_writeCursor, store);
    }

    public ColumnarWriteCursor getWriteCursor() {
        return m_writeCursor;
    }

    // TODO we should make this depend on the size of the data that we know about in advance
    /** max number of rows in one batch */
    private static final int MAX_BATCH_LENGTH = 500;

    /** max size of a batch */
    private static final int MAX_BATCH_SIZE = 5000;

    static class Badger {

        private final BatchWriter m_writer;

        private final BadgerWriteCursor m_writeCursor;

        private final BatchStore m_store;

        Badger(final BadgerWriteCursor cursor, final BatchStore store) {
            m_writeCursor = cursor;
            m_store = store;
            m_writer = m_store.getWriter();
            m_current_batch = m_writer.create(MAX_BATCH_LENGTH);
        }

        private int m_previous_index = 0;

        private WriteBatch m_current_batch;

        void finish() throws IOException {
            writeCurrentBatch();
            m_writer.close();
        }

        /**
         * Write buffered rows to the underlying store. Split batches when they become large enough.
         */
        void tryAdvance() {
            int numCols = m_current_batch.numData();

            // TODO the name is wrong because it is the first that is not yet written, right?
            int lastValidIndex = m_writeCursor.m_current;

            // TODO if we do this in parallel, each write access needs its own current row (probably thread local)
            AtomicInteger currentRow = new AtomicInteger(m_previous_index);

            // Create accesses to be able to write into a WriteBatch
            // TODO the accesses could be created in the constructor and re-used
            ColumnarSchema schema = m_store.getSchema();
            ColumnarWriteAccess[] accessesToTheCurrentBatch = IntStream.range(0, m_store.getSchema().numColumns())//
                .mapToObj(j -> ColumnarAccessFactoryMapper.createAccessFactory(schema.getSpec(j)))//
                .map(f -> f.createWriteAccess(() -> currentRow.get()))// TODO is this right? We have an index in the cursor and one in the batch and we kind of mix them here
                .toArray(ColumnarWriteAccess[]::new);

            // Connect the accesses with the current write batch
            for (int col = 0; col < numCols; col++) {
                accessesToTheCurrentBatch[col].setData(m_current_batch.get(col));
            }

            // Loop to the last row that was written before this method was called
            // and serialize them to the current write batch
            while (currentRow.get() < lastValidIndex) {
                // Set the data from the buffer
                for (int col = 0; col < numCols; ++col) {
                    accessesToTheCurrentBatch[col].setFrom(m_writeCursor.m_access.getAccess(col));
                }

                currentRow.incrementAndGet();

                // TODO use the size tracker by Adrian
                if (m_current_batch.sizeOf() >= MAX_BATCH_SIZE) {
                    try {
                        // TODO if we have written more data in some columns make sure we do not loose it
                        switchToNextBatch();
                        currentRow.set(0);
                    } catch (IOException ex) {
                        // TODO: handle exception
                    }
                }
            }
            m_previous_index = currentRow.get();
        }

        private void writeCurrentBatch() throws IOException {
            m_writer.write(m_current_batch.close(m_previous_index));
        }

        private void switchToNextBatch() throws IOException {
            writeCurrentBatch();

            // Create the next batch
            m_current_batch = m_writer.create(MAX_BATCH_LENGTH);
            m_previous_index = 0;
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

        private long m_offset = 0;

        @Override
        public boolean forward() {
            if (m_current >= 0) {
                m_buffers[m_current].setFrom(m_access);

            }
            m_current++;

            // synchonously write
            m_badger.tryAdvance();

            // Ring buffer
            if (m_current == m_bufferSize) {
                // TODO: when writing asynchronously, we have to make sure that we don't overwrite old
                // buffers that haven't been serialized yet
                m_current = 0;
                m_offset += m_bufferSize;
            }
            return true;
        }

        @Override
        public void flush() throws IOException {
            // TODO Auto-generated method stub
            m_badger.tryAdvance();
            m_badger.switchToNextBatch();
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub
            m_badger.tryAdvance();
            m_badger.finish();
        }

        @Override
        public long getNumForwards() {
            return m_offset + m_current;
        }
    }

}
