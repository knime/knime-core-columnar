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
 *   Jan 24, 2024 (benjamin): created
 */
package org.knime.core.columnar.badger;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.cache.object.ObjectCache;
import org.knime.core.columnar.cache.object.shared.SoftReferencedObjectCache;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A test implementation of a {@link BatchStore} wrapping an {@link ObjectCache} for writing into it.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class WritableObjectCacheTestStore implements BatchStore {

    private final ExecutorService m_persistExecutor;

    private final ExecutorService m_serializeExecutor;

    private final SoftReferencedObjectCache m_heapCache;

    private final ObjectCache m_objectCache;

    WritableObjectCacheTestStore(final ArrowBatchStore store) {
        m_persistExecutor = Executors.newFixedThreadPool(4);
        m_serializeExecutor = Executors.newFixedThreadPool(4);
        m_heapCache = new SoftReferencedObjectCache(); // TODO in reality it is connected to the memory alert system
        m_objectCache = new ObjectCache(store, store, m_heapCache, m_persistExecutor, m_serializeExecutor);
    }

    @Override
    public BatchWriter getWriter() {
        return m_objectCache.getWriter();
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_objectCache.getSchema();
    }

    @Override
    public int numBatches() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileHandle getFileHandle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        throw new UnsupportedOperationException();
    }

    public void flush() throws IOException {
        m_objectCache.flush();
    }

    @Override
    public void close() throws IOException {
        m_objectCache.close();
        m_persistExecutor.shutdownNow();
        m_serializeExecutor.shutdownNow();
        m_heapCache.invalidate();
    }

    @Override
    public long[] getBatchBoundaries() {
        return m_objectCache.getBatchBoundaries();
    }
}
