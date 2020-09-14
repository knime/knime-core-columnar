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
package org.knime.core.columnar;

import static org.knime.core.columnar.store.ColumnStoreUtils.ERROR_MESSAGE_READER_CLOSED;
import static org.knime.core.columnar.store.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.store.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.store.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.columnar.batch.Batch;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestColumnStore implements ColumnStore {

    final class TestColumnDataFactory implements ColumnDataFactory {

        @Override
        public WriteBatch create() {
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            final TestDoubleColumnData[] data = new TestDoubleColumnData[m_schema.getNumColumns()];
            for (int i = 0; i < m_schema.getNumColumns(); i++) {
                data[i] = TestDoubleColumnData.create(m_maxDataCapacity);
                m_tracker.add(data[i]);
            }
            return new DefaultWriteBatch(m_schema, data, m_maxDataCapacity);
        }

    }

    final class TestColumnDataWriter implements ColumnDataWriter {

        @Override
        public void write(final Batch batch) throws IOException {
            if (m_writerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            final Double[][] data = new Double[batch.length()][];
            for (int i = 0; i < data.length; i++) {
                TestDoubleColumnData testData = (TestDoubleColumnData)batch.get(i);
                data[i] = testData.get();
                // last batch might have less values than its max capacity
                if (data[i].length > testData.length()) {
                    data[i] = Arrays.copyOf(data[i], testData.length());
                }
            }

            m_batches.add(data);
        }

        @Override
        public void close() {
            m_writerClosed = true;
        }

    }

    final class TestColumnDataReader implements ColumnDataReader {

        private final ColumnSelection m_selection;

        private boolean m_readerClosed;

        TestColumnDataReader(final ColumnSelection selection) {
            m_selection = selection;
            m_numOpenReaders.incrementAndGet();
        }

        @Override
        public Batch readRetained(final int chunkIndex) throws IOException {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            final Double[][] data = m_batches.get(chunkIndex);
            return m_selection.createBatch(i -> {
                TestDoubleColumnData newData = TestDoubleColumnData.create(data[i]);
                m_tracker.add(newData);
                return newData;
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
            m_numOpenReaders.decrementAndGet();
        }

    }

    private final ColumnStoreSchema m_schema;

    private final int m_maxDataCapacity;

    private final ColumnDataFactory m_factory = new TestColumnDataFactory();

    private final ColumnDataWriter m_writer = new TestColumnDataWriter();

    private final List<Double[][]> m_batches = new ArrayList<>();

    private final List<TestDoubleColumnData> m_tracker = new ArrayList<>();

    private final AtomicInteger m_numOpenReaders = new AtomicInteger();

    // this flag is volatile so that data written by the writer in some thread is visible to a reader in another thread
    private volatile boolean m_writerClosed;

    // this flag is volatile so that when the store is closed in some thread, a reader in another thread will notice
    private volatile boolean m_storeClosed;

    public TestColumnStore(final ColumnStoreSchema schema, final int maxDataCapacity) {
        m_schema = schema;
        m_maxDataCapacity = maxDataCapacity;
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
    public void save(final File f) throws IOException {
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

        return new TestColumnDataReader(selection);
    }

    @Override
    public void close() {
        m_storeClosed = true;

        // check if all memory has been released before closing this store.
        for (final TestDoubleColumnData data : m_tracker) {
            if (data.getRefs() != 0) {
                throw new IllegalStateException("Data not closed.");
            }
        }
    }

    public boolean isStoreClosed() {
        return m_storeClosed;
    }

    public boolean isWriterClosed() {
        return m_writerClosed;
    }

    public int getNumOpenReaders() {
        return m_numOpenReaders.get();
    }

}
