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
 *   22 Apr 2021 (Marc): created
 */
package org.knime.core.columnar.cursor;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactory;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarReadAccess;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.data.ReadDataCache;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link Cursor Cursors} to read from {@link BatchReadStore data storage}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public class ColumnarCursorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDataCache.class);

    /**
     * Creates a {@link LookaheadCursor} that reads from the provided {@link BatchReadStore}.
     *
     * @param store  to read from
     * @param selection  the columns to read
     * @param firstBatchIndex  index of the first batch to read
     * @param lastBatchIndex  index of the last batch to read
     * @param firstIndexInFirstBatch  first index to read in the first batch
     * @param lastIndexInLastBatch  last index to read in the last batch
     * @return a {@link LookaheadCursor} that reads from {@link BatchReadStore store}
     * @throws IOException if a I/O problem occurs
     */
    public static LookaheadCursor<ReadAccessRow> create(final BatchReadStore store, final ColumnSelection selection,
        final int firstBatchIndex, final int lastBatchIndex, final int firstIndexInFirstBatch,
        final int lastIndexInLastBatch) throws IOException {

        var schema = store.getSchema();
        var accessFactories = IntStream.range(0, schema.numColumns())//
            .mapToObj(schema::getSpec)//
            .map(ColumnarAccessFactoryMapper::createAccessFactory)//
            .toArray(ColumnarAccessFactory[]::new);

        if (firstBatchIndex == lastBatchIndex) {
            try (final var reader = selection == null ? store.createRandomAccessReader()
                : store.createRandomAccessReader(selection)) {
                return new SingleBatchColumnarCursor(reader.readRetained(firstBatchIndex), accessFactories,
                    firstIndexInFirstBatch, lastIndexInLastBatch, selection);
            }
        } else {
            return new MultiBatchColumnarCursor(store, accessFactories, firstBatchIndex, lastBatchIndex,
                firstIndexInFirstBatch, lastIndexInLastBatch, selection);
        }
    }

    private static final class SingleBatchColumnarCursor
        implements LookaheadCursor<ReadAccessRow>, ReadAccessRow, ColumnDataIndex {

        private final int m_maxIndex;

        private final int m_numColumns;

        private final NullableReadData[] m_data;

        private final ColumnarReadAccess[] m_accesses;

        private int m_index;

        private ReadBatch m_batch;

        private SingleBatchColumnarCursor(final ReadBatch batch, final ColumnarAccessFactory[] accessFactories,
            final int firstIndexInBatch, final int lastIndexInBatch, final ColumnSelection selection) throws IOException {

            m_batch = batch;
            m_data = m_batch.getUnsafe();
            m_numColumns = m_data.length;

            m_index = firstIndexInBatch - 1;
            m_maxIndex = lastIndexInBatch;

            m_accesses = new ColumnarReadAccess[m_numColumns];
            IntStream indices = IntStream.range(0, m_numColumns);
            if (selection != null) {
                indices = indices.filter(selection::isSelected);
            }
            indices.forEach(i -> {
                m_accesses[i] = accessFactories[i].createReadAccess(this);
                m_accesses[i].setData(batch.get(i));
            });
        }

        @Override
        public boolean canForward() {
            return m_index < m_maxIndex;
        }

        @Override
        public boolean forward() {
            if (m_index >= m_maxIndex) {
                return false;
            } else {
                m_index++;
                return true;
            }
        }

        @Override
        public ReadAccessRow access() {
            return this;
        }

        @Override
        public void close() {
            if (m_batch != null) {
                m_batch.release();
                m_index = m_maxIndex + 1;
                m_batch = null;
            }
        }

        @Override
        public int getNumColumns() {
            return m_numColumns;
        }

        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            @SuppressWarnings("unchecked")
            final A cast = (A)m_accesses[index];
            return cast;
        }

        @Override
        public int getIndex() {
            return m_index;
        }

    }

    private static final class MultiBatchColumnarCursor
        implements LookaheadCursor<ReadAccessRow>, ReadAccessRow, ColumnDataIndex {

        private final int m_numColumns;

        private final RandomAccessBatchReader m_reader;

        private final ColumnSelection m_selection;

        private final ColumnarReadAccess[] m_accesses;

        private final int m_lastBatchIndex;

        private final int m_lastIndexInLastBatch;

        private ReadBatch m_currentBatch;

        private NullableReadData[] m_currentData;

        private int m_currentBatchIndex;

        private int m_currentIndexInCurrentBatch;

        private int m_lastIndexInCurrentBatch;

        @SuppressWarnings("resource")// The batch reader is closed by the cursor
        private MultiBatchColumnarCursor(final BatchReadStore store, final ColumnarAccessFactory[] accessFactories,
            final int firstBatchIndex, final int lastBatchIndex, final int firstIndexInFirstBatch,
            final int lastIndexInLastBatch, final ColumnSelection selection) throws IOException {
            m_selection = selection;
            m_numColumns = store.getSchema().numColumns();

            m_reader = selection == null ? store.createRandomAccessReader()
                : store.createRandomAccessReader(selection);
            m_currentBatchIndex = firstBatchIndex;
            m_lastBatchIndex = lastBatchIndex;
            m_currentIndexInCurrentBatch = firstIndexInFirstBatch - 1;
            m_lastIndexInLastBatch = lastIndexInLastBatch;
            m_accesses = Arrays.stream(accessFactories)//
                .map(f -> f.createReadAccess(this))//
                .toArray(ColumnarReadAccess[]::new);
            readCurrentBatch();
        }

        @Override
        public boolean canForward() {
            return m_currentIndexInCurrentBatch < m_lastIndexInCurrentBatch || m_currentBatchIndex < m_lastBatchIndex;
        }

        @Override
        public boolean forward() {
            if (m_currentIndexInCurrentBatch >= m_lastIndexInCurrentBatch) {
                if (m_currentBatchIndex >= m_lastBatchIndex) {
                    return false;
                }
                m_currentBatch.release();
                m_currentBatchIndex++;
                try {
                    readCurrentBatch();
                } catch (IOException ex) {
                    final String error = "Exception while reading batch.";
                    LOGGER.error(error, ex);
                    throw new IllegalStateException(error, ex);
                }
                m_currentIndexInCurrentBatch = 0;
            } else {
                m_currentIndexInCurrentBatch++;
            }
            return true;
        }

        @Override
        public ReadAccessRow access() {
            return this;
        }

        private void readCurrentBatch() throws IOException {
            m_currentBatch = m_reader.readRetained(m_currentBatchIndex);
            m_currentData = m_currentBatch.getUnsafe();
            for (int i = 0; i < m_numColumns; i++) {
                if (m_selection == null || m_selection.isSelected(i)) {
                    m_accesses[i].setData(m_currentData[i]);
                }
            }
            if (m_currentBatchIndex != m_lastBatchIndex) {
                m_lastIndexInCurrentBatch = m_currentBatch.length() - 1;
            } else {
                m_lastIndexInCurrentBatch = m_lastIndexInLastBatch;
                closeReader();
            }
        }

        private void closeReader() throws IOException {
            m_reader.close();
        }

        @Override
        public int getIndex() {
            return m_currentIndexInCurrentBatch;
        }

        @Override
        public int getNumColumns() {
            return m_numColumns;
        }

        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            @SuppressWarnings("unchecked")
            final A cast = (A)m_accesses[index];
            return cast;
        }

        @Override
        public void close() throws IOException {
            if (m_currentBatch != null) {
                m_currentBatch.release();
                closeReader();
                m_currentBatch = null;
                m_currentIndexInCurrentBatch = m_lastIndexInCurrentBatch + 1;
                m_currentBatchIndex = m_lastBatchIndex;
            }
        }

    }

    private ColumnarCursorFactory() {
    }

}
