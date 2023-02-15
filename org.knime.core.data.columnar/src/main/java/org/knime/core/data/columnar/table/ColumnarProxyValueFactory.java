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
 *   Feb 15, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.knime.core.columnar.access.ColumnarAccessFactory;
import org.knime.core.columnar.access.ColumnarAccessFactoryMapper;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.ProxyRowIterator.ProxyValue;
import org.knime.core.data.v2.RowKeyValueFactory;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.table.access.ReadAccess;

/**
 * Creates ProxyValues that index into a {@link BatchReadStore}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarProxyValueFactory implements Closeable {

    private final RandomAccessProvider m_accessProvider;

    private final ColumnarValueSchema m_valueSchema;

    ColumnarProxyValueFactory(final BatchReadStore store, final ColumnarValueSchema valueSchema) {
        m_accessProvider = new RandomAccessProvider(store);
        m_valueSchema = valueSchema;
    }

    List<ProxyValue<DataCell>> createCellProxies() {
        return IntStream.range(1, m_valueSchema.numColumns())//
                .mapToObj(this::createCellProxy)//
                .collect(toList());
    }

    private ProxyValue<DataCell> createCellProxy(final int column) {
        var valueFactory = m_valueSchema.getValueFactory(column);
        return new ColumnarProxyValue<>(r -> toCell(r, valueFactory), m_accessProvider,
            column);
    }

    private static DataCell toCell(final ReadAccess access, final ValueFactory<ReadAccess, ?> valueFactory) {
        if (access.isMissing()) {
            return DataType.getMissingCell();
        } else {
            return valueFactory.createReadValue(access).getDataCell();
        }
    }

    ProxyValue<RowKey> createKeyProxy() {
        var valueFactory = (RowKeyValueFactory<ReadAccess, ?>)m_valueSchema.getValueFactory(0);
        return new ColumnarProxyValue<>(r -> new RowKey(valueFactory.createReadValue(r).getString()), m_accessProvider,
            0);
    }

    @Override
    public void close() throws IOException {
        m_accessProvider.close();
    }

    private static final class RandomAccessProvider implements Closeable {

        private final BatchReadStore m_store;

        private final RandomAccessBatchReader m_reader;

        private final ColumnarAccessFactory[] m_accessFactories;

        // the batch is cached because reading and releasing is in O(numColumns)
        private ReadBatch m_currentBatch;

        private int m_currentBatchIndex = -1;

        RandomAccessProvider(final BatchReadStore store) {
            m_store = store;
            m_reader = store.createRandomAccessReader();
            m_accessFactories = store.getSchema().specStream()//
                    .map(ColumnarAccessFactoryMapper::createAccessFactory)//
                    .toArray(ColumnarAccessFactory[]::new);
        }

        synchronized <T> T getAccess(final long row, final int column, final Function<ReadAccess, T> accessMapper) {
            final int columnDataIndex = getColumnDataIndex(row);
            var batch = getBatch(row);
            var data = batch.get(column);
            var access = m_accessFactories[column].createReadAccess(() -> columnDataIndex);
            access.setData(data);
            return accessMapper.apply(access);
        }

        private ReadBatch getBatch(final long rowIndex) {
            int batchIndex = getBatchIndex(rowIndex);
            if (batchIndex != m_currentBatchIndex) {
                if (m_currentBatch != null) {
                    m_currentBatch.release();
                }
                m_currentBatch = readBatch(batchIndex);
                m_currentBatchIndex = batchIndex;
            }
            return m_currentBatch;
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

        private int getColumnDataIndex(final long rowIndex) {
            return (int)(rowIndex % m_store.batchLength());
        }

        @Override
        public synchronized void close() throws IOException {
            if (m_currentBatch != null) {
                m_currentBatch.release();
                m_currentBatch = null;
                m_currentBatchIndex = -1;
            }
            m_reader.close();
        }

    }

    private static final class RandomAccessHolder implements AutoCloseable {

        private final ReadAccess m_access;

        private final NullableReadData m_data;

        RandomAccessHolder(final ColumnarAccessFactory accessFactory, final NullableReadData data,
            final int indexInData) {
            var access = accessFactory.createReadAccess(() -> indexInData);
            access.setData(data);
            m_access = access;
            m_data = data;
            m_data.retain();
        }

        @Override
        public void close() {
            m_data.release();
        }

        ReadAccess getAccess() {
            return m_access;
        }

    }

    private static final class ColumnarProxyValue<T> implements ProxyValue<T> {

        private final int m_column;

        private final Function<ReadAccess, T> m_accessMapper;

        private final RandomAccessProvider m_randomAccessProvider;

        ColumnarProxyValue(final Function<ReadAccess, T> accessMapper, final RandomAccessProvider randomAccessProvider,
            final int column) {
            m_column = column;
            m_accessMapper = accessMapper;
            m_randomAccessProvider = randomAccessProvider;
        }

        @Override
        public synchronized T getValue(final long rowIndex) {
            return m_randomAccessProvider.getAccess(rowIndex, m_column, m_accessMapper);
        }

    }

}
