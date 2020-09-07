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

import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_READER_CLOSED;
import static org.knime.core.columnar.ColumnStoreUtils.ERROR_MESSAGE_STORE_CLOSED;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnReadStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnSelection;

/**
 * A {@link ColumnReadStore} that delegates all operations to a delegate store. Similarly, any of its created
 * {@link ColumnDataReader readers} delegate their operations to the readers of the delegate store. Invocations of
 * {@link Closeable#close()} on the store or any of its created readers are also delegated. The purpose of this
 * PhantomReferenceReadStore is to make sure that
 * <ol>
 * <li>when this store or any of its created readers remain unclosed when reclaimed by the garbage collector, the
 * respective delegate store and readers are closed;</li>
 * <li>all of its {@link ColumnDataReader readers} are closed when the store itself is closed.</li>
 * </ol>
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class PhantomReferenceReadStore implements ColumnReadStore {

    private static final class Reader implements ColumnDataReader {

        private final ColumnDataReader m_delegate;

        private final CloseableCloser m_storeClosed;

        private final Set<Reader> m_openReaders;

        // effectively final (set in the static factory method)
        private CloseableCloser m_closed;

        static Reader create(final ColumnDataReader delegate, final CloseableCloser storeClosed,
            final Set<Reader> openReaders) {
            final Reader reader = new Reader(delegate, storeClosed, openReaders);
            openReaders.add(reader);
            reader.m_closed = CloseableCloser.create(reader, delegate, "Column Data Reader");
            return reader;
        }

        private Reader(final ColumnDataReader delegate, final CloseableCloser storeClosed,
            final Set<Reader> openReaders) {
            m_delegate = delegate;
            m_storeClosed = storeClosed;
            m_openReaders = openReaders;
        }

        @Override
        public ColumnData[] read(final int chunkIndex) throws IOException {
            if (m_closed.isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_READER_CLOSED);
            }
            if (m_storeClosed.isClosed()) {
                throw new IllegalStateException(ERROR_MESSAGE_STORE_CLOSED);
            }

            return m_delegate.read(chunkIndex);
        }

        @Override
        public int getNumChunks() {
            return m_delegate.getNumChunks();
        }

        @Override
        public int getMaxDataCapacity() {
            return m_delegate.getMaxDataCapacity();
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
            m_closed.close();
            m_openReaders.remove(this);
        }

    }

    private final ColumnReadStore m_delegate;

    // weakly-hashed
    private final Set<Reader> m_openReaders;

    // effectively final (set in the static factory method)
    private CloseableCloser m_closed;

    /**
     * @param delegate the delegate to which to write
     * @return a new PhantomReferenceReadStore with a registered {@link CloseableCloser}
     */
    public static PhantomReferenceReadStore create(final ColumnReadStore delegate) {
        final PhantomReferenceReadStore store = new PhantomReferenceReadStore(delegate);
        store.m_closed = CloseableCloser.create(store, delegate, "Column Read Store");
        return store;
    }

    private PhantomReferenceReadStore(final ColumnReadStore delegate) {
        m_delegate = delegate;
        m_openReaders = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
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

        return Reader.create(m_delegate.createReader(selection), m_closed, m_openReaders);
    }

    @Override
    public void close() throws IOException {
        m_delegate.close();
        m_closed.close();
        for (final Reader reader : m_openReaders) {
            reader.close();
        }
    }

}
