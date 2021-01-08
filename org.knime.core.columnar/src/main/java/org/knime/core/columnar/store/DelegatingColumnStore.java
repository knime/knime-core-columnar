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
 * <li>initializes its {@link ColumnDataFactory factory}, its {@link ColumnDataWriter writer} and its
 * {@link ColumnDataReader readers} lazily, and</li>
 * <li>provides a method for determining whether the store has been closed already.</li>
 * </ul>
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public abstract class DelegatingColumnStore implements ColumnStore {

    /**
     * A {@link ColumnDataFactory} that delegates operations to another factory. In addition, it
     * <ul>
     * <li>makes sure that operations adhere to their contracts (e.g., that create is not called after the store or
     * writer have been closed) and</li>
     * <li>initializes its delegate factory lazily.</li>
     * </ul>
     */
    public abstract static class DelegatingColumnDataFactory implements ColumnDataFactory {

        private final DelegatingColumnStore m_store;

        private final AtomicBoolean m_storeClosed;

        private final AtomicBoolean m_writerClosed;

        private ColumnDataFactory m_delegate;

        /**
         * @param store a delegating store from which to obtain the delegate factory
         */
        protected DelegatingColumnDataFactory(final DelegatingColumnStore store) {
            m_store = store;
            m_storeClosed = store.m_storeClosed;
            m_writerClosed = store.m_writerClosed;
        }

        @Override
        public final WriteBatch create(final int chunkSize) {
            if (m_writerClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            return createInternal(chunkSize);
        }

        /**
         * Calls {@link ColumnDataFactory#create(int) create} on the delegate factory.
         *
         * @param chunkSize see {@link ColumnDataFactory#create(int)}
         * @return the result of the delegated operation
         */
        protected WriteBatch createInternal(final int chunkSize) {
            return initAndGetDelegate().create(chunkSize);
        }

        /**
         * Initializes the delegate factory, if it has not been initialized before, and returns it.
         *
         * @return the delegate factory
         */
        protected ColumnDataFactory initAndGetDelegate() {
            if (m_delegate == null) {
                m_delegate = m_store.m_delegate.getFactory();
            }
            return m_delegate;
        }

        /**
         * @return the delegate factory, if it has been initialized, otherwise null
         */
        protected ColumnDataFactory getDelegate() {
            return m_delegate;
        }

    }

    /**
     * A {@link ColumnDataWriter} that delegates operations to another writer. In addition, it
     * <ul>
     * <li>makes sure that operations adhere to their contracts (e.g., that write is not called after close and that
     * close is idempotent),</li>
     * <li>initializes its delegate writer lazily, and</li>
     * <li>provides a method for determining whether the writer has been closed already.</li>
     * </ul>
     */
    public abstract static class DelegatingColumnDataWriter implements ColumnDataWriter {

        private final DelegatingColumnStore m_store;

        private final AtomicBoolean m_storeClosed;

        private final AtomicBoolean m_writerClosed;

        private ColumnDataWriter m_delegate;

        /**
         * @param store a delegating store from which to obtain the delegate writer
         */
        protected DelegatingColumnDataWriter(final DelegatingColumnStore store) {
            m_store = store;
            m_storeClosed = store.m_storeClosed;
            m_writerClosed = store.m_writerClosed;
        }

        @Override
        public final void write(final ReadBatch batch) throws IOException {
            if (m_writerClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed.get()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            writeInternal(batch);
        }

        /**
         * Calls {@link ColumnDataWriter#write(ReadBatch) write} on the delegate writer.
         *
         * @param batch see {@link ColumnDataWriter#write(ReadBatch)}
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
            if (m_delegate != null) {
                m_delegate.close();
            }
        }

        /**
         * Initializes the delegate writer, if it has not been initialized before, and returns it.
         *
         * @return the delegate writer
         */
        protected ColumnDataWriter initAndGetDelegate() {
            if (m_delegate == null) {
                m_delegate = m_store.m_delegate.getWriter();
            }
            return m_delegate;
        }

        /**
         * @return the delegate writer, if it has been initialized, otherwise null
         */
        protected ColumnDataWriter getDelegate() {
            return m_delegate;
        }

        /**
         * @return true if this writer has been closed, otherwise false
         */
        protected boolean isClosed() {
            return m_writerClosed.get();
        }

    }

    /**
     * A {@link ColumnDataReader} that delegates operations to another reader. In addition, it
     * <ul>
     * <li>makes sure that operations adhere to their contracts (e.g., that readRetained is not called after the store
     * has been closed and that close is idempotent),</li>
     * <li>initializes its delegate reader lazily, and</li>
     * <li>provides a method for determining whether the reader has been closed already.</li>
     * </ul>
     */
    public abstract static class DelegatingColumnDataReader implements ColumnDataReader {

        private final DelegatingColumnStore m_store;

        private final ColumnSelection m_selection;

        private final AtomicBoolean m_storeClosed;

        private ColumnDataReader m_delegate;

        private boolean m_readerClosed;

        /**
         * @param store a delegating store from which to obtain the delegate reader
         * @param selection see {@link ColumnReadStore#createReader(ColumnSelection)}
         */
        protected DelegatingColumnDataReader(final DelegatingColumnStore store, final ColumnSelection selection) {
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

            return readRetainedInternal(index);
        }

        /**
         * Calls {@link ColumnDataReader#readRetained(int) readRetained} on the delegate reader.
         *
         * @param index see {@link ColumnDataReader#readRetained(int)}
         * @throws IOException if an I/O error occurs
         * @return see {@link ColumnDataReader#readRetained(int)}
         */
        @SuppressWarnings("resource")
        protected ReadBatch readRetainedInternal(final int index) throws IOException {
            return initAndGetDelegate().readRetained(index);
        }

        @SuppressWarnings("resource")
        @Override
        public int getNumBatches() throws IOException {
            return initAndGetDelegate().getNumBatches();
        }

        @SuppressWarnings("resource")
        @Override
        public int getMaxLength() throws IOException {
            return initAndGetDelegate().getMaxLength();
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
        protected ColumnSelection getSelection() {
            return m_selection;
        }

        /**
         * Initializes the delegate reader, if it has not been initialized before, and returns it.
         *
         * @return the delegate reader
         */
        protected ColumnDataReader initAndGetDelegate() {
            if (m_delegate == null) {
                m_delegate = m_store.m_delegate.createReader(m_selection);
            }
            return m_delegate;
        }

        /**
         * @return the delegate reader, if it has been initialized, otherwise null
         */
        protected ColumnDataReader getDelegate() {
            return m_delegate;
        }

        /**
         * @return true if this writer has been closed, otherwise false
         */
        protected boolean isClosed() {
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

    private ColumnDataFactory m_factory;

    private ColumnDataWriter m_writer;

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
    public final ColumnDataFactory getFactory() {
        if (m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        if (m_factory == null) {
            m_factory = createFactoryInternal();
        }
        return m_factory;
    }

    /**
     * Calls {@link ColumnWriteStore#getFactory() getFactory} on the delegate store.
     *
     * @return see {@link ColumnWriteStore#getFactory()}
     */
    protected ColumnDataFactory createFactoryInternal() {
        return m_delegate.getFactory();
    }

    @Override
    public final ColumnDataWriter getWriter() {
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
    protected ColumnDataWriter createWriterInternal() {
        return m_delegate.getWriter();
    }

    @Override
    public final void save(final File f) throws IOException {
        if (!m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        saveInternal(f);
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
    public final ColumnDataReader createReader(final ColumnSelection config) {
        if (!m_writerClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_storeClosed.get()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return createReaderInternal(config);
    }

    /**
     * Calls {@link ColumnReadStore#createReader() createReader} on the delegate store.
     *
     * @param selection see {@link ColumnReadStore#createReader()}
     * @return see {@link ColumnReadStore#createReader()}
     */
    protected ColumnDataReader createReaderInternal(final ColumnSelection selection) {
        return m_delegate.createReader(selection);
    }

    @Override
    public final void close() throws IOException {
        if (!m_storeClosed.getAndSet(true)) {
            closeOnce();
            if (m_writer != null) {
                m_writer.close();
            }
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
    protected ColumnStore getDelegate() {
        return m_delegate;
    }

    /**
     * @return true if this store has been closed, otherwise false
     */
    protected boolean isClosed() {
        return m_storeClosed.get();
    }

}
