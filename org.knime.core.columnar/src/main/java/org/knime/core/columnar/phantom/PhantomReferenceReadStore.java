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

import static org.knime.core.columnar.store.ColumnStoreUtils.ERROR_MESSAGE_READER_CLOSED;
import static org.knime.core.columnar.store.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;

import java.io.Closeable;
import java.io.IOException;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 * A {@link ColumnReadStore} that delegates all operations to a delegate store. Similarly, any of its created
 * {@link ColumnDataReader readers} delegate their operations to the readers of the delegate store. Invocations of
 * {@link Closeable#close()} on the store or any of its created readers are also delegated. The purpose of this
 * PhantomReferenceReadStore is to make sure that when this store or any of its created readers remain unclosed when
 * reclaimed by the garbage collector, the respective delegate store and readers are closed.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class PhantomReferenceReadStore implements ColumnReadStore {

    private static final class Reader implements ColumnDataReader {

        private final ColumnDataReader m_delegate;

        private final CloseableHandler m_storeClosed;

        // effectively final (set in the static factory method)
        private CloseableDelegateFinalizer m_closed;

        static Reader create(final ColumnDataReader delegate, final CloseableHandler storeClosed) {
            final Reader reader = new Reader(delegate, storeClosed);
            reader.m_closed = CloseableDelegateFinalizer.create(reader, delegate, "Column Data Reader");
            return reader;
        }

        private Reader(final ColumnDataReader delegate, final CloseableHandler storeClosed) {
            m_delegate = delegate;
            m_storeClosed = storeClosed;
        }

        @Override
        public ReadBatch readRetained(final int chunkIndex) throws IOException {
            if (m_closed.isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed.isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            return m_delegate.readRetained(chunkIndex);
        }

        @Override
        public int getNumBatches() throws IOException {
            return m_delegate.getNumBatches();
        }

        @Override
        public int getMaxLength() throws IOException {
            return m_delegate.getMaxLength();
        }

        @Override
        public void close() throws IOException {
            m_closed.close();
            m_delegate.close();
        }

    }

    private final ColumnReadStore m_delegate;

    // effectively final (set in the static factory method)
    private CloseableHandler m_closed;

    /**
     * @param delegate the delegate to which to write
     * @return a new PhantomReferenceReadStore with a registered {@link CloseableDelegateFinalizer}
     */
    public static PhantomReferenceReadStore create(final ColumnReadStore delegate) {
        final PhantomReferenceReadStore store = new PhantomReferenceReadStore(delegate);
        store.m_closed = CloseableDelegateFinalizer.create(store, delegate, "Column Read Store");
        return store;
    }

    private PhantomReferenceReadStore(final ColumnReadStore delegate) {
        m_delegate = delegate;
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    @SuppressWarnings("resource")
    public ColumnDataReader createReader(final ColumnSelection selection) {
        if (m_closed.isClosed()) {
            throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
        }

        return Reader.create(m_delegate.createReader(selection), m_closed);
    }

    @Override
    public void close() throws IOException {
        m_closed.close();
        m_delegate.close();
    }

}
