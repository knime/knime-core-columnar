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
 *   20 Aug 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.phantom;

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_WRITER_NOT_CLOSED;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnStore} that delegates all operations to a delegate store. Similarly, its {@link ColumnDataWriter
 * writer} and any of its created {@link ColumnDataReader readers} delegate their operations to the writer and readers
 * of the delegate store. Invocations of {@link Closeable#close()} on the store, its writer or any of its created
 * readers are also delegated. The purpose of this PhantomReferenceStore is to make sure that
 * <ol>
 * <li>when this store, its writer, or any of its created readers remain unclosed when reclaimed by the garbage
 * collector, the respective delegate store, writer, and readers are closed;</li>
 * <li>its {@link ColumnDataWriter writer} and all {@link ColumnDataReader readers} are closed when the store itself is
 * closed.</li>
 * </ol>
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class PhantomReferenceStore implements ColumnStore {

    private static final class Factory implements ColumnDataFactory {

        private final ColumnDataFactory m_delegate;

        private final CloseableHandler m_writerClosed;

        private final CloseableHandler m_storeClosed;

        Factory(final ColumnDataFactory delegate, final CloseableHandler writerClosed,
            final CloseableHandler storeClosed) {
            m_delegate = delegate;
            m_writerClosed = writerClosed;
            m_storeClosed = storeClosed;
        }

        @Override
        public ColumnData[] create() {
            if (m_writerClosed.isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed.isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            return m_delegate.create();
        }

    }

    private static final class Writer implements ColumnDataWriter {

        private final ColumnDataWriter m_delegate;

        private final CloseableHandler m_storeClosed;

        // effectively final (set in the static factory method)
        private CloseableHandler m_closed;

        static Writer create(final ColumnDataWriter delegate, final CloseableHandler storeClosed) {
            final Writer writer = new Writer(delegate, storeClosed);
            writer.m_closed = CloseableDelegateFinalizer.create(writer, delegate, "Column Data Writer");
            return writer;
        }

        private Writer(final ColumnDataWriter delegate, final CloseableHandler storeClosed) {
            m_delegate = delegate;
            m_storeClosed = storeClosed;
        }

        @Override
        public void write(final ColumnData[] record) throws IOException {
            if (m_closed.isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
            }
            if (m_storeClosed.isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            m_delegate.write(record);
        }

        @Override
        public void close() throws IOException {
            m_closed.close();
            m_delegate.close();
        }

    }

    private final ColumnStore m_delegate;

    private final PhantomReferenceReadStore m_readStore;

    // effectively final (set in the static factory method)
    private CloseableHandler m_closed;

    private Writer m_writer;

    private CloseableHandler m_writerClosed;

    private Factory m_factory;

    /**
     * @param delegate the delegate to which to write
     * @return a new PhantomReferenceStore with a registered {@link CloseableDelegateFinalizer}
     */
    @SuppressWarnings("resource")
    public static PhantomReferenceStore create(final ColumnStore delegate) {
        final PhantomReferenceStore store = new PhantomReferenceStore(delegate);
        store.m_closed = CloseableDelegateFinalizer.create(store, delegate, "Column Store");
        store.m_writer = Writer.create(delegate.getWriter(), store.m_closed);
        store.m_writerClosed = store.m_writer.m_closed;
        store.m_factory = new Factory(delegate.getFactory(), store.m_writerClosed, store.m_closed);
        return store;
    }

    private PhantomReferenceStore(final ColumnStore delegate) {
        m_delegate = delegate;
        m_readStore = PhantomReferenceReadStore.create(delegate);
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_readStore.getSchema();
    }

    @Override
    public ColumnDataFactory getFactory() {
        if (m_writerClosed.isClosed()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_closed.isClosed()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_factory;
    }

    @Override
    public ColumnDataWriter getWriter() {
        if (m_writerClosed.isClosed()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_CLOSED);
        }
        if (m_closed.isClosed()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return m_writer;
    }

    @Override
    public void saveToFile(final File f) throws IOException {
        if (!m_writerClosed.isClosed()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }
        if (m_closed.isClosed()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        m_delegate.saveToFile(f);
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection config) {
        if (!m_writerClosed.isClosed()) {
            throw new IllegalStateException(ERROR_MESSAGE_WRITER_NOT_CLOSED);
        }

        return m_readStore.createReader(config);
    }

    @Override
    public void close() throws IOException {
        m_closed.close();
        m_writer.close();
        m_readStore.close();
        m_delegate.close();
    }

}
