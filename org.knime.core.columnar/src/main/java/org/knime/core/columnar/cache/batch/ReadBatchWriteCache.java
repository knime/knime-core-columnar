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
 *   Apr 6, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.batch;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.cache.batch.SharedReadBatchCache.BatchId;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * {@link BatchWritable} that caches the {@link ReadBatch ReadBatches} that are written to it and synchronously writes
 * them into another {@link BatchWritable}. {@link RandomAccessBatchReader Readers} of this cache will fail with an
 * {@link IndexOutOfBoundsException} if a batch index exceeds the number of batches that are already written.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ReadBatchWriteCache implements BatchWritable, RandomAccessBatchReadable {

    private final BatchWritable m_writeDelegate;

    private final RandomAccessBatchReadable m_readDelegate;

    private final CachingBatchWriter m_writer;

    private final SharedReadBatchCache m_cache;

    private final ColumnSelection m_allColumns;

    private final AtomicBoolean m_closed = new AtomicBoolean(false);

    /**
     * Constructs a new ReadBatchWriteCache.
     *
     * @param writeDelegate to synchronously write ReadBatches to
     * @param readDelegate to read from on cache miss
     * @param cache to cache ReadBatches in
     */
    public ReadBatchWriteCache(final BatchWritable writeDelegate, final RandomAccessBatchReadable readDelegate,
        final SharedReadBatchCache cache) {
        m_writeDelegate = writeDelegate;
        m_cache = cache;
        var writer = new CachingBatchWriter(writeDelegate.getWriter());
        m_writer = writer;
        m_readDelegate = readDelegate;
        m_allColumns = new DefaultColumnSelection(m_writeDelegate.getSchema().numColumns());
    }

    private void checkOpen() {
        if (m_closed.get()) {
            throw new IllegalStateException("The ReadBatchWriteCache is already closed.");
        }
    }

    @Override
    public void close() throws IOException {
        if (!m_closed.getAndSet(true)) {
            m_cache.evictAllFromSource(this);
            m_writer.close();
            m_readDelegate.close();
        }
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        try {
            m_writer.m_awaitClose.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for writer to be closed.");
        }
        return new CachingBatchReader(selection);
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader() {
        // use the already existing selection instead of letting the default implementation create a new one
        return createRandomAccessReader(m_allColumns);
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_writeDelegate.getSchema();
    }

    private final class CachingBatchWriter implements BatchWriter {

        private BatchWriter m_delegateWriter;

        private final AtomicInteger m_numBatches = new AtomicInteger();

        private final CountDownLatch m_awaitClose = new CountDownLatch(1);

        CachingBatchWriter(final BatchWriter writer) {
            m_delegateWriter = writer;
        }

        @Override
        public void close() throws IOException {
            if (m_delegateWriter != null) {
                m_delegateWriter.close();
                // remove the reference as this writer will live longer than the delegate needs to
                m_delegateWriter = null;
            }
            m_awaitClose.countDown();
        }

        @Override
        public WriteBatch create(final int capacity) {
            checkOpen();
            return m_delegateWriter.create(capacity);
        }

        @Override
        public void write(final ReadBatch batch) throws IOException {
            checkOpen();
            batch.retain();
            m_cache.put(new BatchId(ReadBatchWriteCache.this, m_allColumns, m_numBatches.getAndIncrement()), batch);
            m_delegateWriter.write(batch);
        }

    }

    private final class CachingBatchReader implements RandomAccessBatchReader {

        private final RandomAccessBatchReader m_reader;

        private final ColumnSelection m_selection;

        // AP-15959: Assumes that no more batches are written -> needs to be changed once we allow reading while writing
        private final LocalReadBatchCache m_localCache =
            new LocalReadBatchCache(this::readFromSharedCache, m_writer.m_numBatches.get());

        CachingBatchReader(final ColumnSelection selection) {
            m_selection = selection;
            m_reader = m_readDelegate.createRandomAccessReader(selection);
        }

        @Override
        public void close() throws IOException {
            m_reader.close();
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            checkOpen();
            if (index >= m_writer.m_numBatches.get()) {
                throw new IndexOutOfBoundsException(index);
            }
            return m_localCache.readRetained(index);
        }

        private ReadBatch readFromSharedCache(final int index) throws IOException {
            var id = new BatchId(ReadBatchWriteCache.this, m_selection, index);
            return m_cache.getRetained(id, () -> getFullOrReadFromDelegate(index));
        }

        private ReadBatch getFullOrReadFromDelegate(final int index) throws IOException {
            // having more data is not a problem therefore we can use the full batch which might still be in the cache
            // from writing
            var id = new BatchId(ReadBatchWriteCache.this, m_allColumns, index);
            return m_cache.getRetained(id)//
                    .orElseGet(() -> readFromDelegate(index));
        }

        private ReadBatch readFromDelegate(final int index) {
            try {
                return m_reader.readRetained(index);
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to read batch from delegate.", ex);
            }
        }

    }

}
