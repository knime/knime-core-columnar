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
package org.knime.core.columnar.testing;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.testing.data.TestData;
import org.knime.core.columnar.testing.data.TestDataFactory;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class DefaultTestBatchStore implements TestBatchStore {

    private static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

    private static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    final class TestBatchWriter implements BatchWriter {

        @Override
        public WriteBatch create(final int capacity) {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            waitForLatch();
            final NullableWriteData[] data = new NullableWriteData[m_schema.numColumns()];
            for (int i = 0; i < m_factories.length; i++) {
                final TestData testData = m_factories[i].createWriteData(capacity);
                data[i] = testData;
                m_tracker.add(testData);
            }
            return new DefaultWriteBatch(data);
        }

        @Override
        public void write(final ReadBatch batch) {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            waitForLatch();
            final Object[][] data = new Object[batch.numData()][];
            for (int i = 0; i < data.length; i++) {
                final TestData testData = (TestData)batch.get(i);
                data[i] = testData.get();
            }
            if (m_batches.isEmpty()) {
                m_maxDataLength = batch.length();
            }
            if (m_maxDataLength != batch.length()) {
                throw new IllegalStateException("All written batches must have same length.");
            }
            m_batches.add(data);
        }

        @Override
        public void close() {
            m_writerClosed = true;
            ColumnarTest.OPEN_CLOSEABLES.remove(TestBatchWriter.this);
        }

    }

    final class TestBatchReader implements RandomAccessBatchReader {

        private final ColumnSelection m_selection;

        private boolean m_readerClosed;

        TestBatchReader(final ColumnSelection selection) {
            m_selection = selection;
        }

        @Override
        public ReadBatch readRetained(final int chunkIndex) {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            waitForLatch();
            final Object[][] data = m_batches.get(chunkIndex);
            return m_selection.createBatch(i -> {
                final TestData testData = m_factories[i].createReadData(data[i], m_maxDataLength);
                m_tracker.add(testData);
                return testData;
            });
        }

        @Override
        public void close() {
            m_readerClosed = true;
            ColumnarTest.OPEN_CLOSEABLES.remove(TestBatchReader.this);
        }

    }

    private final ColumnarSchema m_schema;

    private final TestDataFactory[] m_factories;

    private final TestBatchWriter m_writer = new TestBatchWriter();

    private final List<Object[][]> m_batches = new ArrayList<>();

    private final List<TestData> m_tracker = new ArrayList<>();

    // this flag is volatile so that data written by the writer in some thread is visible to a reader in another thread
    private volatile boolean m_writerClosed;

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    private int m_maxDataLength = -1;

    private CountDownLatch m_latch;

    public static DefaultTestBatchStore create(final ColumnarSchema schema) {
        final DefaultTestBatchStore store = new DefaultTestBatchStore(schema);
        ColumnarTest.OPEN_CLOSEABLES.add(store);
        ColumnarTest.OPEN_CLOSEABLES.add(store.m_writer);
        return store;
    }

    private DefaultTestBatchStore(final ColumnarSchema schema) {
        m_schema = schema;
        m_factories = IntStream.range(0, schema.numColumns()) //
            .mapToObj(i -> schema.getSpec(i).accept(TestSchemaMapper.INSTANCE, schema.getTraits(i)) )//
            .toArray(TestDataFactory[]::new);
    }

    @Override
    public void blockOnCreateWriteRead(final CountDownLatch latch) {
        m_latch = latch;
    }

    private void waitForLatch() {
        if (m_latch != null) {
            try {
                m_latch.await();
            } catch (InterruptedException e) {
                // Restore interrupted state
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @Override
    public BatchWriter getWriter() {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_writer;
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        final TestBatchReader reader = new TestBatchReader(selection);
        ColumnarTest.OPEN_CLOSEABLES.add(reader);
        return reader;
    }

    @Override
    public void close() {
        m_writer.close();
        m_storeClosed = true;

        // check if all memory has been released before closing this store.
        for (final TestData data : m_tracker) {
            assertEquals("Data not closed.", 0, data.getRefs());
        }

        ColumnarTest.OPEN_CLOSEABLES.remove(this);
        ColumnarTest.storeClosedLatch.countDown();
    }

    @Override
    public List<TestData> getData() {
        return m_tracker;
    }

    @Override
    public int numBatches() {
        return m_batches.size();
    }

    @Override
    public int batchLength() {
        return m_maxDataLength;
    }

}
