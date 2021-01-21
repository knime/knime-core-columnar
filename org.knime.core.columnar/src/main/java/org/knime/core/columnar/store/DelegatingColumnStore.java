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
 *   17 Nov 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.store;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.filter.ColumnSelection;

/**
 * A {@link ColumnStore} that delegates operations to another store. In addition, it
 * <ul>
 * <li>makes sure that operations adhere to their contracts (e.g. that the writer is a singleton, that readers are only
 * created after the writer has been closed, and that close is idempotent),</li>
 * <li>initializes its {@link BatchFactory factory}, its {@link BatchWriter writer} and its {@link BatchReader readers}
 * lazily, and</li>
 * <li>provides a method for determining whether the store has been closed already.</li>
 * </ul>
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public abstract class DelegatingColumnStore implements ColumnStore {

    /**
     * A {@link BatchFactory} that delegates operations to another factory. In addition, it
     * <ul>
     * <li>makes sure that operations adhere to their contracts (e.g., that create is not called after the store or
     * writer have been closed) and</li>
     * <li>initializes its delegate factory lazily.</li>
     * </ul>
     */
    public abstract static class DelegatingBatchFactory implements BatchFactory {

        private final DelegatingColumnStore m_store;

        private final AtomicBoolean m_storeClosed;

        private final AtomicBoolean m_writerClosed;

        private BatchFactory m_delegate;

        /**
         * @param store a delegating store from which to obtain the delegate factory
         */
        protected DelegatingBatchFactory(final DelegatingColumnStore store) {
            m_store = store;
            m_storeClosed = store.m_storeClosed;
            m_writerClosed = store.m_writerClosed;
        }

        @Override
        public final WriteBatch create(final int capacity) {
            if (m_writerClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            return createInternal(capacity);
        }

        /**
         * Calls {@link BatchFactory#create(int) create} on the delegate factory.
         *
         * @param capacity see {@link BatchFactory#create(int)}
         * @return the result of the delegated operation
         */
        protected WriteBatch createInternal(final int capacity) {
            return initAndGetDelegate().create(capacity);
        }

        /**
         * Initializes the delegate factory, if it has not been initialized before, and returns it.
         *
         * @return the delegate factory
         */
        protected final BatchFactory initAndGetDelegate() {
            if (m_delegate == null) {
                m_delegate = m_store.m_delegate.getFactory();
            }
            return m_delegate;
        }

        /**
         * @return the delegate factory, if it has been initialized, otherwise null
         */
        protected final BatchFactory getDelegate() {
            return m_delegate;
        }

    }

    /**
     * A {@link BatchWriter} that delegates operations to another writer. In addition, it
     * <ul>
     * <li>makes sure that operations adhere to their contracts (e.g., that write is not called after close and that
     * close is idempotent),</li>
     * <li>initializes its delegate writer lazily, and</li>
     * <li>provides a method for determining whether the writer has been closed already.</li>
     * </ul>
     */
    public abstract static class DelegatingBatchWriter implements BatchWriter {

        private final DelegatingColumnStore m_store;

        private final AtomicBoolean m_storeClosed;

        private final AtomicBoolean m_writerClosed;

        private BatchWriter m_delegate;

        /**
         * @param store a delegating store from which to obtain the delegate writer
         */
        protected DelegatingBatchWriter(final DelegatingColumnStore store) {
            m_store = store;
            m_storeClosed = store.m_storeClosed;
            m_writerClosed = store.m_writerClosed;
        }

        @Override
        public final void write(final ReadBatch batch) throws IOException {
            if (m_storeClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }
            if (m_writerClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }

            writeInternal(batch);
        }

        /**
         * Calls {@link BatchWriter#write(ReadBatch) write} on the delegate writer.
         *
         * @param batch see {@link BatchWriter#write(ReadBatch)}
         * @throws IOException if an I/O error occurs
         */
        @SuppressWarnings("resource")
        protected void writeInternal(final ReadBatch batch) throws IOException {
            initAndGetDelegate().write(batch);
        }

        @Override
        public final void close() throws IOException {
            if (!m_writerClosed.getAndSet(true)) {
                closeOnce();
            }
        }

        /**
         * Calls {@link Closeable#close() close} on the delegate writer. When overriding this method, make sure to close
         * the delegate writer, if it has been initialized.
         *
         * @throws IOException if an I/O error occurs
         */
        protected void closeOnce() throws IOException {
            initAndGetDelegate().close();
        }

        /**
         * Initializes the delegate writer, if it has not been initialized before, and returns it.
         *
         * @return the delegate writer
         */
        protected final BatchWriter initAndGetDelegate() {
            if (m_delegate == null) {
                m_delegate = m_store.m_delegate.getWriter();
            }
            return m_delegate;
        }

        /**
         * @return the delegate writer, if it has been initialized, otherwise null
         */
        protected final BatchWriter getDelegate() {
            return m_delegate;
        }

        /**
         * @return true if this writer has been closed, otherwise false
         */
        protected final boolean isClosed() {
            return m_writerClosed.get();
        }

    }

    /**
     * A {@link BatchReader} that delegates operations to another reader. In addition, it
     * <ul>
     * <li>makes sure that operations adhere to their contracts (e.g., that readRetained is not called after the store
     * has been closed and that close is idempotent),</li>
     * <li>initializes its delegate reader lazily, and</li>
     * <li>provides a method for determining whether the reader has been closed already.</li>
     * </ul>
     */
    public abstract static class DelegatingBatchReader implements BatchReader {

        private final DelegatingColumnStore m_store;

        private final ColumnSelection m_selection;

        private final AtomicBoolean m_storeClosed;

        private BatchReader m_delegate;

        private boolean m_readerClosed;

        /**
         * @param store a delegating store from which to obtain the delegate reader
         * @param selection see {@link ColumnReadStore#createReader(ColumnSelection)}
         */
        protected DelegatingBatchReader(final DelegatingColumnStore store, final ColumnSelection selection) {
            m_store = store;
            m_selection = selection;
            m_storeClosed = store.m_storeClosed;
        }

        @Override
        public final ReadBatch readRetained(final int index) throws IOException {
            if (m_readerClosed) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }
            if (index < 0) {
                throw new IndexOutOfBoundsException(String.format("Batch index %d smaller than 0.", index));
            }
            if (index >= numBatches()) {
                throw new IndexOutOfBoundsException(
                    String.format("Batch index %d greater or equal to the reader's largest batch index (%d).", index,
                        numBatches() - 1));
            }

            return readRetainedInternal(index);
        }

        /**
         * Calls {@link BatchReader#readRetained(int) readRetained} on the delegate reader.
         *
         * @param index see {@link BatchReader#readRetained(int)}
         * @throws IOException if an I/O error occurs
         * @return see {@link BatchReader#readRetained(int)}
         */
        @SuppressWarnings("resource")
        protected ReadBatch readRetainedInternal(final int index) throws IOException {
            return initAndGetDelegate().readRetained(index);
        }

        @SuppressWarnings("resource")
        @Override
        public int numBatches() throws IOException {
            return initAndGetDelegate().numBatches();
        }

        @SuppressWarnings("resource")
        @Override
        public int maxLength() throws IOException {
            return initAndGetDelegate().maxLength();
        }

        @Override
        public final void close() throws IOException {
            if (!m_readerClosed) {
                m_readerClosed = true;
                closeOnce();
            }
        }

        /**
         * Calls {@link Closeable#close() close} on the delegate reader. When overriding this method, make sure to close
         * the delegate reader, if it has been initialized.
         *
         * @throws IOException if an I/O error occurs
         */
        protected void closeOnce() throws IOException {
            if (m_delegate != null) {
                m_delegate.close();
            }
        }

        /**
         * @return the column selection for this reader
         */
        protected final ColumnSelection getSelection() {
            return m_selection;
        }

        /**
         * Initializes the delegate reader, if it has not been initialized before, and returns it.
         *
         * @return the delegate reader
         */
        protected final BatchReader initAndGetDelegate() {
            if (m_delegate == null) {
                m_delegate = m_store.m_delegate.createReader(m_selection);
            }
            return m_delegate;
        }

        /**
         * @return the delegate reader, if it has been initialized, otherwise null
         */
        protected final BatchReader getDelegate() {
            return m_delegate;
        }

        /**
         * @return true if this writer has been closed, otherwise false
         */
        protected final boolean isClosed() {
            return m_readerClosed;
        }

    }

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

    private static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

    private static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    private final ColumnStore m_delegate;

    private final AtomicBoolean m_storeClosed = new AtomicBoolean();

    private final AtomicBoolean m_writerClosed = new AtomicBoolean();

    private BatchFactory m_factory;

    private BatchWriter m_writer;

    /**
     * @param delegate the store to which to delegate operations
     */
    protected DelegatingColumnStore(final ColumnStore delegate) {
        m_delegate = delegate;
    }

    @Override
    public final ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public final BatchFactory getFactory() {
        if (m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        if (m_factory == null) {
            m_factory = getFactoryInternal();
        }
        return m_factory;
    }

    /**
     * Calls {@link ColumnWriteStore#getFactory() getFactory} on the delegate store.
     *
     * @return see {@link ColumnWriteStore#getFactory()}
     */
    protected BatchFactory getFactoryInternal() {
        return new DelegatingBatchFactory(this) {
        };
    }

    @Override
    public final BatchWriter getWriter() {
        if (m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        if (m_writer == null) {
            m_writer = createWriterInternal();
        }
        return m_writer;
    }

    /**
     * Calls {@link ColumnWriteStore#getWriter() getWriter} on the delegate store.
     *
     * @return see {@link ColumnWriteStore#getWriter()}
     */
    protected BatchWriter createWriterInternal() {
        return new DelegatingBatchWriter(this) {
        };
    }

    @Override
    public final void save(final File file) throws IOException {
        if (!m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        saveInternal(file);
    }

    /**
     * Calls {@link ColumnWriteStore#save(File) saveInternal} on the delegate store.
     *
     * @param f see {@link ColumnWriteStore#save(File)}
     * @throws IOException if an I/O error occurs
     */
    protected void saveInternal(final File f) throws IOException {
        m_delegate.save(f);
    }

    @Override
    public final BatchReader createReader(final ColumnSelection selection) {
        if (!m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return createReaderInternal(selection);
    }

    /**
     * Calls {@link ColumnReadStore#createReader() createReader} on the delegate store.
     *
     * @param selection see {@link ColumnReadStore#createReader()}
     * @return see {@link ColumnReadStore#createReader()}
     */
    protected BatchReader createReaderInternal(final ColumnSelection selection) {
        return new DelegatingBatchReader(this, selection) {
        };
    }

    @Override
    public final void close() throws IOException {
        if (!m_storeClosed.getAndSet(true)) {
            if (m_writer != null) {
                m_writer.close();
            }
            closeOnce();
        }
    }

    /**
     * Calls {@link Closeable#close() close} on the delegate store. When overriding this method, make sure to close the
     * delegate store.
     *
     * @throws IOException if an I/O error occurs
     */
    protected void closeOnce() throws IOException {
        m_delegate.close();
    }

    /**
     * @return the delegate store
     */
    protected final ColumnStore getDelegate() {
        return m_delegate;
    }

    /**
     * @return true if this store has been closed, otherwise false
     */
    protected final boolean isClosed() {
        return m_storeClosed.get();
    }

}
