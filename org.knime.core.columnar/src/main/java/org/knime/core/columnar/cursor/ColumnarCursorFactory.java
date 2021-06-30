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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactory;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarReadAccess;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Creates {@link Cursor Cursors} to read from {@link BatchReadStore data storages}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ColumnarCursorFactory {

    /**
     * Creates a {@link LookaheadCursor} that reads from the provided {@link BatchReadStore}.
     *
     * @param store to read from
     * @param selection the columns to read
     * @param firstBatchIndex index of the first batch to read
     * @param lastBatchIndex index of the last batch to read
     * @param firstIndexInFirstBatch first index to read in the first batch
     * @param lastIndexInLastBatch last index to read in the last batch. Can be {@code -1} in which case the last batch
     *            is read fully.
     * @return a {@link LookaheadCursor} that reads from {@link BatchReadStore store}
     */
    public static LookaheadCursor<ReadAccessRow> create(final BatchReadStore store, final ColumnSelection selection,
        final int firstBatchIndex, final int lastBatchIndex, final int firstIndexInFirstBatch,
        final int lastIndexInLastBatch) {

        final ColumnarSchema schema = store.getSchema();
        final ColumnarAccessFactory[] accessFactories = IntStream.range(0, schema.numColumns()) //
            .mapToObj(schema::getSpec) //
            .map(ColumnarAccessFactoryMapper::createAccessFactory) //
            .toArray(ColumnarAccessFactory[]::new);

        return new DefaultColumnarCursor(store, selection, accessFactories, firstBatchIndex, lastBatchIndex,
            firstIndexInFirstBatch, lastIndexInLastBatch);
    }

    private ColumnarCursorFactory() {}

    private static final class DefaultColumnarCursor
        implements LookaheadCursor<ReadAccessRow>, ReadAccessRow, ColumnDataIndex {

        private final int m_numColumns;

        private final ColumnSelection m_selection;

        private final ColumnarReadAccess[] m_accesses;

        private final RandomAccessBatchReader m_reader;

        private final int m_firstBatchIndex;

        private final int m_firstIndexInFirstBatch;

        private final int m_lastBatchIndex;

        private final int m_lastIndexInLastBatch;

        private int m_currentBatchIndex = -1;

        private ReadBatch m_currentBatch;

        private int m_lastIndexInCurrentBatch = -1;

        private int m_currentIndexInCurrentBatch;

        private DefaultColumnarCursor(final BatchReadStore store, final ColumnSelection selection,
            final ColumnarAccessFactory[] accessFactories, final int firstBatchIndex, final int lastBatchIndex,
            final int firstIndexInFirstBatch, final int lastIndexInLastBatch) {
            m_numColumns = store.getSchema().numColumns();
            m_selection = selection;
            m_accesses = Arrays.stream(accessFactories) //
                .map(f -> f.createReadAccess(this)) //
                .toArray(ColumnarReadAccess[]::new);

            @SuppressWarnings("resource") // The batch reader is closed when this instance is closed.
            final RandomAccessBatchReader reader = selection == null //
                ? store.createRandomAccessReader() //
                : store.createRandomAccessReader(selection);
            m_reader = reader;

            m_firstBatchIndex = firstBatchIndex;
            m_firstIndexInFirstBatch = firstIndexInFirstBatch;
            // Special case for stores that contain only empty batches: don't even bother trying to iterate over them,
            // this would only complicate the index-handling logic in the methods below.
            m_lastBatchIndex = store.batchLength() > 0 ? lastBatchIndex : -1;
            m_lastIndexInLastBatch = lastIndexInLastBatch;

            m_currentBatchIndex = firstBatchIndex - 1;
            m_currentIndexInCurrentBatch = firstIndexInFirstBatch - 1;
        }

        @Override
        public ReadAccessRow access() {
            return this;
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
                if (m_currentBatch != null) {
                    m_currentBatch.release();
                }
                m_currentBatchIndex++;
                readCurrentBatch();
                m_currentIndexInCurrentBatch = m_currentBatchIndex == m_firstBatchIndex //
                    ? m_firstIndexInFirstBatch //
                    : 0;
            } else {
                m_currentIndexInCurrentBatch++;
            }
            return true;
        }

        private void readCurrentBatch() {
            try {
                m_currentBatch = m_reader.readRetained(m_currentBatchIndex);
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
            final NullableReadData[] currentData = m_currentBatch.getUnsafe();
            for (int i = 0; i < m_numColumns; i++) {
                if (m_selection == null || m_selection.isSelected(i)) {
                    m_accesses[i].setData(currentData[i]);
                }
            }
            if (m_currentBatchIndex != m_lastBatchIndex
            // Indicates that the last batch should be read fully.
                || m_lastIndexInLastBatch == -1) {
                m_lastIndexInCurrentBatch = m_currentBatch.length() - 1;
            } else {
                m_lastIndexInCurrentBatch = m_lastIndexInLastBatch;
            }
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
                m_currentBatch = null;
            }
            m_reader.close();
        }

    }
}
