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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.columnar.testing.data.TestData;
import org.knime.core.columnar.testing.data.TestDataFactory;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestColumnStore implements ColumnStore {

    private static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

    private static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    final class TestColumnDataFactory implements ColumnDataFactory {

        @Override
        public WriteBatch create(final int chunkSize) {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            waitForLatch();
            final ColumnWriteData[] data = new ColumnWriteData[m_schema.getNumColumns()];
            for (int i = 0; i < m_factories.length; i++) {
                final TestData testData = m_factories[i].createWriteData(chunkSize);
                data[i] = testData;
                m_tracker.add(testData);
            }
            return new DefaultWriteBatch(data);
        }

    }

    final class TestColumnDataWriter implements ColumnDataWriter {

        private int m_maxDataLength;

        @Override
        public void write(final ReadBatch batch) throws IOException {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            waitForLatch();
            final Object[] data = new Object[batch.getNumColumns()];
            for (int i = 0; i < data.length; i++) {
                final TestData testData = (TestData)batch.get(i);
                data[i] = testData.get();
            }
            if (m_batches.size() == 0) {
                m_maxDataLength = batch.length();
            } else if (m_maxDataLength != batch.length()) {
                throw new IllegalStateException("All written batches must have same length.");
            }
            m_batches.add(data);
        }

        @Override
        public void close() {
            m_writerClosed = true;
            ColumnarTest.OPEN_CLOSEABLES.remove(TestColumnDataWriter.this);
        }

    }

    final class TestColumnDataReader implements ColumnDataReader {

        private final ColumnSelection m_selection;

        private final int m_maxDataCapacity;

        private boolean m_readerClosed;

        TestColumnDataReader(final ColumnSelection selection, final int maxCapacity) {
            m_selection = selection;
            m_maxDataCapacity = maxCapacity;
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
            final Object[] data = m_batches.get(chunkIndex);
            return ColumnSelection.createBatch(m_selection, i -> {
                final TestData testData = m_factories[i].createReadData(data[i]);
                m_tracker.add(testData);
                return testData;
            });
        }

        @Override
        public int getNumBatches() {
            return m_batches.size();
        }

        @Override
        public int getMaxLength() {
            return m_maxDataCapacity;
        }

        @Override
        public void close() {
            m_readerClosed = true;
            ColumnarTest.OPEN_CLOSEABLES.remove(TestColumnDataReader.this);
        }

    }

    private final ColumnStoreSchema m_schema;

    private final TestDataFactory[] m_factories;

    private final ColumnDataFactory m_factory = new TestColumnDataFactory();

    private final TestColumnDataWriter m_writer = new TestColumnDataWriter();

    private final List<Object[]> m_batches = new ArrayList<>();

    private final List<TestData> m_tracker = new ArrayList<>();

    // this flag is volatile so that data written by the writer in some thread is visible to a reader in another thread
    private volatile boolean m_writerClosed;

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    private CountDownLatch m_latch;

    public static TestColumnStore create(final ColumnStoreSchema schema) {
        final TestColumnStore store = new TestColumnStore(schema);
        ColumnarTest.OPEN_CLOSEABLES.add(store);
        ColumnarTest.OPEN_CLOSEABLES.add(store.m_writer);
        return store;
    }

    private TestColumnStore(final ColumnStoreSchema schema) {
        m_schema = schema;
        m_factories = IntStream.range(0, schema.getNumColumns()) //
            .mapToObj(schema::getColumnDataSpec) //
            .map(s -> s.accept(TestSchemaMapper.INSTANCE)) //
            .toArray(TestDataFactory[]::new);
    }

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
    public ColumnStoreSchema getSchema() {
        return m_schema;
    }

    @Override
    public ColumnDataFactory getFactory() {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_factory;
    }

    @Override
    public ColumnDataWriter getWriter() {
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_writer;
    }

    @Override
    public void save(final File f) {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        throw new UnsupportedOperationException("Saving to file not supported by test column store.");
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection selection) {
        if (!m_writerClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        final TestColumnDataReader reader = new TestColumnDataReader(selection, m_writer.m_maxDataLength);
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
    }

    public List<TestData> getData() {
        return m_tracker;
    }

}
