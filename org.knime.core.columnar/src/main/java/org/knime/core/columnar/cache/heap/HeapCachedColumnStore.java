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
 *   Oct 8, 2020 (dietzc): created
 */
package org.knime.core.columnar.cache.heap;

import java.io.File;
import java.io.IOException;

import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 * TODO Marc: should we just make this part of the cache, due to the 'restrictions' below.
 *
 * Object column store interceptor for in-heap caching of object columns. Needs to be wrapped around an off-heap cache
 * and has to be inside the domain calculation. Reasoning: the objects are cached and only forwarded to the off-heap
 * cache AFTER full serialization.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class HeapCachedColumnStore implements ColumnStore {

    private final ColumnStore m_delegate;

    private final int[] m_selection;

    private final HeapCachedColumnReadStore m_readStore;

    /**
     * Create a new Object {@link ColumnStore}.
     *
     * @param delegate store. Requests will be forwarded to that store.
     */
    public HeapCachedColumnStore(final ColumnStore delegate) {
        m_delegate = delegate;
        m_selection = HeapCacheUtils.getObjectDataIndices(delegate.getSchema());
        m_readStore = new HeapCachedColumnReadStore(delegate, m_selection);
    }

    @SuppressWarnings("resource")
    @Override
    public ColumnDataWriter getWriter() {
        return new HeapCachedColumnStoreWriter(m_delegate.getWriter(), m_selection);
    }

    @Override
    public ColumnDataFactory getFactory() {
        return new HeapCacheColumnStoreDataFactory(m_delegate.getFactory(), m_delegate.getSchema(), m_selection);
    }

    @Override
    public void save(final File f) throws IOException {
        m_delegate.save(f);
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection config) {
        return m_readStore.createReader(config);
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public void close() throws IOException {
        m_delegate.close();
        m_readStore.close();
    }

    private static class HeapCacheColumnStoreDataFactory implements ColumnDataFactory {

        private final int[] m_selection;

        private final ColumnDataFactory m_delegate;

        private final ColumnStoreSchema m_schema;

        private HeapCacheColumnStoreDataFactory(final ColumnDataFactory delegate, final ColumnStoreSchema schema,
            final int[] selection) {
            m_delegate = delegate;
            m_schema = schema;
            m_selection = selection;
        }

        @Override
        public WriteBatch create() {
            final WriteBatch batch = m_delegate.create();
            final ColumnWriteData[] data = new ColumnWriteData[m_schema.getNumColumns()];

            // we assume sorting in selection
            int selectionIndex = 0;
            for (int i = 0; i < data.length; i++) {
                if (selectionIndex < m_selection.length && m_selection[selectionIndex] == i) {
                    final ObjectWriteData<?> columnWriteData = (ObjectWriteData<?>)batch.get(i);
                    data[i] = new HeapCachedWriteData<>(columnWriteData);
                    selectionIndex++;
                } else {
                    data[i] = batch.get(i);
                }
            }
            return new DefaultWriteBatch(data, batch.capacity());
        }
    }

    private static class HeapCachedColumnStoreWriter implements ColumnDataWriter {

        private final ColumnDataWriter m_delegate;

        private final int[] m_selection;

        private HeapCachedColumnStoreWriter(final ColumnDataWriter delegate, final int[] selection) {
            m_delegate = delegate;
            m_selection = selection;
        }

        @Override
        public void close() throws IOException {
            // TODO close what ever needs to be closed for this guy, e.g. cache etc
            m_delegate.close();
        }

        @Override
        public void write(final ReadBatch batch) throws IOException {
            // TODO wait until flushed.

            final ColumnReadData[] data = new ColumnReadData[batch.getNumColumns()];
            int selectionIndex = 0;
            for (int i = 0; i < data.length; i++) {
                if (selectionIndex < m_selection.length && m_selection[selectionIndex] == i) {
                    final HeapCachedReadData<?> heapCachedData = (HeapCachedReadData<?>)batch.get(i);
                    data[i] = heapCachedData.getDelegate();
                    selectionIndex++;
                } else {
                    data[i] = batch.get(i);
                }
            }
            m_delegate.write(new DefaultReadBatch(data, batch.length()));
        }

    }
}
