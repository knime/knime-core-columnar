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
 *   Feb 26, 2024 (benjamin): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarWriteAccess;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.DefaultColumnarBatchStore.ColumnarBatchStoreBuilder;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A simple write table. The main difference to {@link ColumnarRowWriteTable} is that the caller must end batches
 * manually.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarCopyFromWriteTable implements RowWriteAccessible {

    private final ColumnarValueSchema m_schema;

    private final DefaultColumnarBatchStore m_store;

    private final CopyFromWriteCursor m_writeCursor;

    public ColumnarCopyFromWriteTable(final ColumnarValueSchema schema, final ColumnStoreFactory storeFactory) {
        m_schema = schema;
        m_store = new ColumnarBatchStoreBuilder(storeFactory.createStore(m_schema, new TempFileHandle())) //
            .enableDictEncoding(false) // TODO what do we need here?
            .useColumnDataCache(ColumnarPreferenceUtils.getColumnDataCache(),
                ColumnarPreferenceUtils.getPersistExecutor()) //
            .build();
        m_writeCursor = new CopyFromWriteCursor();
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    // NOTE: This cursor does not have the usual contract because the caller has to do the batching manually
    // Should we not use the same method?
    @Override
    public CopyFromWriteCursor getWriteCursor() {
        return m_writeCursor;
    }

    // TODO adapt to Tobi's WriteCursor changes
    // TODO public for extended API?
    public class CopyFromWriteCursor implements WriteCursor<WriteAccessRow>, WriteAccessRow {

        private final BatchWriter m_writer;

        private final AtomicInteger m_dataIndex;

        private final ColumnarWriteAccess[] m_accesses;

        private WriteBatch m_batch;

        public CopyFromWriteCursor() {
            m_writer = m_store.getWriter();

            // TODO consolidate with code from the HeapBadger - it also connects data to accesses
            m_dataIndex = new AtomicInteger(0);
            m_accesses = IntStream.range(0, m_schema.numColumns())//
                .mapToObj(i -> ColumnarAccessFactoryMapper.createAccessFactory(m_schema.getSpec(i)))//
                .map(f -> f.createWriteAccess(m_dataIndex::get))//
                .toArray(ColumnarWriteAccess[]::new);
        }

        /**
         * THIS IS THE EXTENDED API - The caller can and must do the batch switching manually
         *
         * @throws IOException
         */
        public void switchToNextBatch(final int numRows) throws IOException {
            if (m_batch != null) {
                var readBatch = m_batch.close(m_dataIndex.get()); // TODO index of by one error?
                m_writer.write(readBatch);
            }
            m_dataIndex.set(0);
            m_batch = m_writer.create(numRows);

            for (var i = 0; i < m_accesses.length; i++) {
                m_accesses[i].setData(m_batch.get(i));
            }
        }

        @Override
        public WriteAccessRow access() {
            return this;
        }

        @Override
        public boolean forward() {
            m_dataIndex.incrementAndGet();
            return true; // We can always write more HAHAHA
        }

        @Override
        public int size() {
            return m_schema.numColumns();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <A extends WriteAccess> A getWriteAccess(final int index) {
            return (A)m_accesses[index];
        }

        @Override
        public void setFrom(final ReadAccessRow row) {
            for (var i = 0; i < m_accesses.length; i++) {
                m_accesses[i].setFrom(row.getAccess(i));
            }
        }

        public void setFrom(final MagicReadAccessRow row) {
            row.pushDownSet(m_batch, m_dataIndex.get());
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub

        }

        // TODO Use this for ending batches????
        @Override
        public void flush() throws IOException {
            // TODO Auto-generated method stub
        }

    }

    // This marks a read row that can accept datas and write to the directly by calling an underlying data API for that
    public interface MagicReadAccessRow extends ReadAccessRow {
        void pushDownSet(WriteBatch batch, int index);
    }
}
