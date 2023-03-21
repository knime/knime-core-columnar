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
 *   Mar 17, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.knime.core.columnar.access.ColumnDataIndex;
import org.knime.core.columnar.access.ColumnarAccessFactory;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.access.ColumnarReadAccess;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.table.access.ReadAccess;

/**
 * Provides random access via row index on a BatchReadStore.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class RandomAccessProvider implements Closeable {

    private final BatchReadStore m_store;

    private final RandomAccessBatchReader m_reader;

    private final ColumnarAccessFactory[] m_accessFactories;

    private final Map<Integer, ReadBatch> m_cache = new ConcurrentHashMap<>();

    private final long m_numRows;

    RandomAccessProvider(final BatchReadStore store, final long numRows) {
        m_store = store;
        m_reader = store.createRandomAccessReader();
        m_accessFactories = store.getSchema().specStream()//
            .map(ColumnarAccessFactoryMapper::createAccessFactory)//
            .toArray(ColumnarAccessFactory[]::new);
        m_numRows = numRows;
    }

    long numRows() {
        return m_numRows;
    }

    RandomReadAccessRow getRandomReadAccessRow(final long rowIndex) {
        var batchIndex = getBatchIndex(rowIndex);
        var indexInBatch = getIndexInBatch(rowIndex);
        return new RandomReadAccessRow(batchIndex, indexInBatch);
    }

    <T> T getAccess(final long row, final int column, final Function<ReadAccess, T> accessMapper) {
        final int columnDataIndex = getIndexInBatch(row);
        var batch = getBatch(row);
        var data = batch.get(column);
        var access = m_accessFactories[column].createReadAccess(() -> columnDataIndex);
        access.setData(data);
        return accessMapper.apply(access);
    }

    private ReadBatch getBatch(final long rowIndex) {
        int batchIndex = getBatchIndex(rowIndex);
        return getBatchByIndex(batchIndex);
    }

    private ReadBatch getBatchByIndex(final int batchIndex) {
        return m_cache.computeIfAbsent(batchIndex, this::readBatch);
    }

    private ReadBatch readBatch(final int batchIndex) {
        try {
            return m_reader.readRetained(batchIndex);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to read the batch at index " + batchIndex, ex);
        }
    }

    private int getBatchIndex(final long rowIndex) {
        return (int)(rowIndex / m_store.batchLength());
    }

    private int getIndexInBatch(final long rowIndex) {
        return (int)(rowIndex % m_store.batchLength());
    }

    @Override
    public synchronized void close() throws IOException {
        for (var batch : m_cache.values()) {
            batch.release();
        }
        m_cache.clear();
        m_reader.close();
    }

    final class RandomReadAccessRow implements ColumnDataIndex {

        private final ColumnarReadAccess[] m_accesses;

        private final int m_batchIndex;

        private final int m_indexInBatch;

        private WeakReference<ReadBatch> m_batch = new WeakReference<>(null);

        RandomReadAccessRow(final int batchIndex, final int indexInBatch) {
            m_batchIndex = batchIndex;
            m_indexInBatch = indexInBatch;
            m_accesses = Stream.of(m_accessFactories)//
                    .map(f -> f.createFixedReadAccess(indexInBatch))//
                    .toArray(ColumnarReadAccess[]::new);
        }

        ReadAccess getAccess(final int accessIndex) {
            return getAccessInternal(accessIndex);
        }

        private ColumnarReadAccess getAccessInternal(final int accessIndex) {
            return m_accesses[accessIndex];
            /*
            if (access != null) {
                return access;
            } else {
                synchronized(m_accesses) {
                    // check again in case another thread initialized the access in the meantime
                    access = m_accesses[accessIndex];
                    if (access == null) {
                        access = m_accessFactories[accessIndex].createReadAccess(this);
                        m_accesses[accessIndex] = access;
                    }
                    return access;
                }
            }
            */
        }


        void runWithInitializedAccess(final int accessIndex, final Runnable runnable) {
            applyOnRetainedBatch( b -> {
                getInitializedAccess(accessIndex, b);
                runnable.run();
                return null;
            });
        }

        <T> T supplyWithInitializedAccess(final int accessIndex, final Supplier<T> supplier) {
            return applyOnRetainedBatch(b -> supplyWithInitializedAccess(accessIndex, supplier, b));
        }


        private <T> T supplyWithInitializedAccess(final int accessIndex, final Supplier<T> supplier,
            final ReadBatch retainedBatch) {
            getInitializedAccess(accessIndex, retainedBatch);
            return supplier.get();
        }

        int size() {
            return m_accesses.length;
        }

        private <T> T applyOnRetainedBatch(final Function<ReadBatch, T> function) {
            var batch = getBatch();
            try {
                return function.apply(batch);
            } finally {
//                batch.release();
            }
        }

        <T> T applyOnAccess(final Function<ReadAccess, T> function, final int accessIndex) {
            return applyOnRetainedBatch(b -> function.apply(getInitializedAccess(accessIndex, b)));
        }

        void acceptAccess(final Consumer<ReadAccess> consumer, final int accessIndex) {
            applyOnRetainedBatch(b -> {
                consumer.accept(getInitializedAccess(accessIndex, b));
                return null;
            });
        }

        private ColumnarReadAccess getInitializedAccess(final int accessIndex, final ReadBatch batch) {
            var access = getAccessInternal(accessIndex);
            access.setData(batch.get(accessIndex));
            return access;
        }

        private ReadBatch getBatch() {
            var batch = m_batch.get();
            if (notReleased(batch)) {
                // batch is not garbage collected, and not released yet
                return batch;
            } else {
                synchronized (this) {
                    // try again because another thread might have initialized it in the meantime
                    batch = m_batch.get();
                    if (notReleased(batch)) {
                        return batch;
                    }
                    batch = getBatchByIndex(m_batchIndex);
                    m_batch = new WeakReference<>(batch);
                    return batch;
                }
            }
        }

        private boolean notReleased(final ReadBatch batch) {
            // the current cache implementation guarantees that a batch is retained
            // if it is not garbage collected because batches are never evicted.
            // TODO check if the batch can be retained once the cache is properly implemented
            return batch != null /*&& batch.tryRetain()*/;
        }

        @Override
        public int getIndex() {
            return m_indexInBatch;
        }

    }

}