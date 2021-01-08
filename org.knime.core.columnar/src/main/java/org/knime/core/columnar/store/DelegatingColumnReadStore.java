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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.filter.ColumnSelection;

/**
 * A {@link ColumnReadStore} that delegates operations to another store. In addition, it
 * <ul>
 * <li>makes sure that operations adhere to their contracts (e.g. that readers are not created after the store has been
 * closed and that close is idempotent),</li>
 * <li>initializes its {@link ColumnDataReader readers} lazily, and</li>
 * <li>provides a method for determining whether the store has been closed already.</li>
 * </ul>
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public abstract class DelegatingColumnReadStore implements ColumnReadStore {

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

        private final DelegatingColumnReadStore m_store;

        private final ColumnSelection m_selection;

        private final AtomicBoolean m_storeClosed;

        private ColumnDataReader m_delegate;

        private boolean m_readerClosed;

        /**
         * @param store a delegating store from which to obtain the delegate reader
         * @param selection see {@link ColumnReadStore#createReader(ColumnSelection)}
         */
        protected DelegatingColumnDataReader(final DelegatingColumnReadStore store, final ColumnSelection selection) {
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

        @Override
        @SuppressWarnings("resource")
        public final int getNumBatches() throws IOException {
            return initAndGetDelegate().getNumBatches();
        }

        @Override
        @SuppressWarnings("resource")
        public final int getMaxLength() throws IOException {
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

    private static final String ERROR_MESSAGE_STORE_CLOSED = "Column read store has already been closed.";

    private static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    private final ColumnReadStore m_delegate;

    private final AtomicBoolean m_storeClosed = new AtomicBoolean();

    /**
     * @param delegate the store to which to delegate operations
     */
    protected DelegatingColumnReadStore(final ColumnReadStore delegate) {
        m_delegate = delegate;
    }

    @Override
    public final ColumnDataReader createReader(final ColumnSelection config) {
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
    public final ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public final void close() throws IOException {
        if (!m_storeClosed.getAndSet(true)) {
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
    protected ColumnReadStore getDelegate() {
        return m_delegate;
    }

    /**
     * @return true if this store has been closed, otherwise false
     */
    protected boolean isClosed() {
        return m_storeClosed.get();
    }

}
