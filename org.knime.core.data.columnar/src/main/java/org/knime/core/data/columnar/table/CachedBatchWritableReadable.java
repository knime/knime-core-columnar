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
package org.knime.core.data.columnar.table;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.cache.data.ReadDataCache;
import org.knime.core.columnar.cache.data.SharedReadDataCache;
import org.knime.core.columnar.cache.object.ObjectCache;
import org.knime.core.columnar.cache.object.SharedObjectCache;
import org.knime.core.columnar.cache.writable.BatchWritableCache;
import org.knime.core.columnar.cache.writable.SharedBatchWritableCache;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.util.memory.MemoryAlert;
import org.knime.core.data.util.memory.MemoryAlertListener;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link BatchWritable} and {@link RandomAccessBatchReadable} that delegates operations through
 * <ul>
 * <li>an {@link ObjectCache},</li>
 * <li>a {@link BatchWritableCache}, and</li>
 * <li>a {@link ReadDataCache}.</li>
 * </ul>
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @param <D>
 *
 * @noreference This class is not intended to be referenced by clients
 */
public final class CachedBatchWritableReadable<D extends BatchWritable & RandomAccessBatchReadable>
    implements BatchWritable, RandomAccessBatchReadable, Flushable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(CachedBatchWritableReadable.class);

    private final D m_delegate;

    private final ReadDataCache m_dataCached;

    private final BatchWritableCache m_smallCached;

    private final ObjectCache m_objectCached;

    private final MemoryAlertListener m_memListener;

    private final BatchWriter m_writer;

    /**
     * Create a {@link CachedBatchWritableReadable} wrapping the given delegate in a series of caches.
     *
     * @param delegate The delegate that provides {@link BatchWritable} and {@link RandomAccessBatchReadable} interfaces.
     */
    public CachedBatchWritableReadable(final D delegate) {
        m_delegate = delegate;

        final SharedReadDataCache columnDataCache = ColumnarPreferenceUtils.getColumnDataCache();
        final SharedBatchWritableCache smallTableCache = ColumnarPreferenceUtils.getSmallTableCache();

        if (columnDataCache.getMaxSizeInBytes() > 0) {
            m_dataCached = new ReadDataCache(delegate, columnDataCache, ColumnarPreferenceUtils.getPersistExecutor());
        } else {
            m_dataCached = null;
        }

        if (smallTableCache.getCacheSize() > 0) {
            if (m_dataCached != null) {
                m_smallCached = new BatchWritableCache(m_dataCached, smallTableCache);
            } else {
                m_smallCached = new BatchWritableCache(delegate, smallTableCache);
            }
        } else {
            m_smallCached = null;
        }

        final SharedObjectCache heapCache = ColumnarPreferenceUtils.getHeapCache();
        final ExecutorService executor = ColumnarPreferenceUtils.getPersistExecutor();
        final ExecutorService serializeExecutor = ColumnarPreferenceUtils.getSerializeExecutor();
        if (m_smallCached != null) {
            m_objectCached = new ObjectCache(m_smallCached, heapCache, executor, serializeExecutor);
        } else if (m_dataCached != null) {
            m_objectCached = new ObjectCache(m_dataCached, heapCache, executor, serializeExecutor);
        } else {
            m_objectCached = new ObjectCache(delegate, heapCache, executor, serializeExecutor);
        }
        m_memListener = new MemoryAlertListener() {
            @Override
            protected boolean memoryAlert(final MemoryAlert alert) {
                new Thread(() -> {
                    try {
                        m_objectCached.flush();
                    } catch (IOException ex) {
                        LOGGER.error("Error during enforced premature serialization of object data.", ex);
                    }
                }).start();
                return false;
            }
        };
        MemoryAlertSystem.getInstanceUncollected().addListener(m_memListener);

        m_writer = m_objectCached.getWriter();
    }

    @Override
    public final ColumnarSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public final BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public final void flush() throws IOException {
        if (m_smallCached != null) {
            m_smallCached.flush();
        }
        if (m_dataCached != null) {
            m_dataCached.flush();
        }
        m_objectCached.flush();
    }

    @Override
    public final RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return m_objectCached.createRandomAccessReader(selection);
    }

    @Override
    public final void close() throws IOException {
        m_writer.close();
        MemoryAlertSystem.getInstanceUncollected().removeListener(m_memListener);
        m_objectCached.close();
    }

}
