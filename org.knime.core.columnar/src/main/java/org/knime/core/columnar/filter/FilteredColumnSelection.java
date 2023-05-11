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
 *   9 Sep 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.filter;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.ReferenceCounter;
import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Implementation of {@link ColumnSelection}, in which only some columns are selected.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class FilteredColumnSelection implements ColumnSelection {

    private final int m_numColumns;

    private final Set<Integer> m_indices;

    /**
     * @param numColumns the total number of columns
     * @param indices the selected columns
     */
    public FilteredColumnSelection(final int numColumns, final int... indices) {
        m_numColumns = numColumns;
        m_indices = Arrays.stream(indices).filter(i -> i >= 0 && i < m_numColumns).boxed().collect(Collectors.toSet());
    }

    @Override
    public boolean isSelected(final int index) {
        return m_indices.contains(index);
    }

    @Override
    public int numColumns() {
        return m_numColumns;
    }

    @Override
    public ReadBatch createBatch(final IntFunction<NullableReadData> function) {
        return new FilteredReadBatch(m_indices.stream().collect(Collectors.toMap(Function.identity(), function::apply)),
            m_numColumns);
    }

    /**
     * A ReadBatch that doesn't contain the all columns of the underlying store.
     */
    public static final class FilteredReadBatch implements ReadBatch {

        private final Map<Integer, NullableReadData> m_data;

        private final int m_numColumns;

        private final int m_length;

        private final ReferenceCounter m_refCounter;

        private final AtomicLong m_size = new AtomicLong(-1);

        /**
         * Constructor.
         *
         * @param data the data stored in the batch
         * @param numColumns the total number of columns in the underlying store
         */
        public FilteredReadBatch(final Map<Integer, NullableReadData> data, final int numColumns) {
            this(data, numColumns, new ReferenceCounter(),
                data.values().stream().mapToInt(ReadData::length).max().orElse(0));
        }

        private FilteredReadBatch(final Map<Integer, NullableReadData> data, final int numColumns,
            final ReferenceCounter refCounter, final int length) {
            m_data = data;
            m_numColumns = numColumns;
            m_length = length;
            m_refCounter = refCounter;
        }

        @Override
        public ReadBatch decorate(final DataDecorator transformer) {
            var transformedData = m_data.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> transformer.decorate(e.getKey(), e.getValue())));
            return new FilteredReadBatch(transformedData, m_numColumns, m_refCounter, m_length);
        }

        @Override
        public NullableReadData get(final int index) {
            final NullableReadData data = m_data.get(index);
            if (data != null) {
                return data;
            }
            if (index < 0) {
                throw new IndexOutOfBoundsException(String.format("Column index %d smaller than 0.", index));
            }
            if (index >= m_numColumns) {
                throw new IndexOutOfBoundsException(String
                    .format("Column index %d larger then the batch's's number of columns (%d).", index, m_numColumns - 1));
            }
            throw new NoSuchElementException(
                String.format("Data at index %d is not available in this filtered batch.", index));
        }

        @Override
        public boolean isMissing(final int index) {
            return !m_data.containsKey(index);
        }

        /**
         * Obtains an array of all {@link NullableWriteData} in this batch. This implementation of the method is safe,
         * since the array it returns is a defensive copy of the data structure underlying this batch
         *
         * @return the non-null array of all data in this batch
         */
        @Override
        public NullableReadData[] getUnsafe() {
            final var data = new NullableReadData[m_numColumns];
            m_data.entrySet().stream().forEach(e -> data[e.getKey()] = e.getValue());
            return data;
        }

        @Override
        public int length() {
            return m_length;
        }

        @Override
        public void release() {
            if (m_refCounter.release()) {
                m_data.values().forEach(ReferencedData::release);
            }
        }

        @Override
        public void retain() {
            m_refCounter.retain();
        }

        @Override
        public boolean tryRetain() {
            return m_refCounter.tryRetain();
        }

        @Override
        public long sizeOf() {
            return m_size
                .updateAndGet(s -> s == -1 ? m_data.values().stream().mapToLong(ReferencedData::sizeOf).sum() : s);
        }

        @Override
        public int numData() {
            return m_numColumns;
        }

    }

}
