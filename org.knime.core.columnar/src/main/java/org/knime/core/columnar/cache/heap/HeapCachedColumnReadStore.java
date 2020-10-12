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
 *   Oct 9, 2020 (dietzc): created
 */
package org.knime.core.columnar.cache.heap;

import java.io.IOException;

import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 * TODO
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class HeapCachedColumnReadStore implements ColumnReadStore {

    private final ColumnReadStore m_delegate;

    private final int[] m_selection;

    public HeapCachedColumnReadStore(final ColumnReadStore delegate) {
        this(delegate, HeapCacheUtils.getObjectDataIndices(delegate.getSchema()));
    }

    public HeapCachedColumnReadStore(final ColumnReadStore delegate, final int[] selection) {
        m_delegate = delegate;
        m_selection = selection;
    }

    @SuppressWarnings("resource")
    @Override
    public ColumnDataReader createReader(final ColumnSelection config) {
        return new HeapCachedColumnDataReader(m_delegate.createReader(config), m_selection);
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public void close() throws IOException {
        m_delegate.close();
    }

    private static class HeapCachedColumnDataReader implements ColumnDataReader {

        private final int[] m_selection;

        private final ColumnDataReader m_delegate;

        private HeapCachedColumnDataReader(final ColumnDataReader delegate, final int[] selection) {
            m_delegate = delegate;
            m_selection = selection;
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
        }

        @Override
        public ReadBatch readRetained(final int index) throws IOException {
            final ReadBatch batch = m_delegate.readRetained(index);
            final ColumnReadData[] data = new ColumnReadData[batch.getNumColumns()];

            // TODO do we have to do this for each read? :-(
            int selectionIndex = 0;
            for (int i = 0; i < data.length; i++) {
                final ColumnReadData readData = batch.get(i);
                if (selectionIndex < m_selection.length && m_selection[selectionIndex] == i
                    && !(readData instanceof HeapCachedReadData)) {
                    final ObjectReadData<?> columnReadData = (ObjectReadData<?>)batch.get(i);
                    data[i] = new HeapCachedReadData<>(columnReadData);
                    selectionIndex++;
                } else {
                    data[i] = batch.get(i);
                }
            }
            return new DefaultReadBatch(data, batch.length());
        }

        @Override
        public int getNumBatches() throws IOException {
            return m_delegate.getNumBatches();
        }

        @Override
        public int getMaxLength() throws IOException {
            return m_delegate.getMaxLength();
        }
    }

}
