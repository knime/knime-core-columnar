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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.table.RandomAccessProvider.RandomReadAccessRow;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.table.access.ReadAccess;

/**
 * Creates random access based DataRows that materialize cells on-demand by retrieving the values from the underlying
 * store.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class RandomAccessDataRowFactory {

    private final RandomAccessProvider m_accessProvider;

    private final ColumnarValueSchema m_valueSchema;

    private final long m_numRows;

    RandomAccessDataRowFactory(final RandomAccessProvider randomAccessProvider, final ColumnarValueSchema valueSchema) {
        m_accessProvider = randomAccessProvider;
        m_valueSchema = valueSchema;
        m_numRows = randomAccessProvider.numRows();
    }

    CloseableRowIterator createIterator() {
        return new RandomAccessRowIterator();
    }

    private RandomAccessDataRow createRandomAccessDataRow(final long rowIndex) {
        return new RandomAccessDataRow(m_accessProvider.getRandomReadAccessRow(rowIndex));
    }

    private final class RandomAccessRowIterator extends CloseableRowIterator {

        private long m_rowIndex;

        @Override
        public boolean hasNext() {
            return m_rowIndex < m_numRows;
        }

        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return createRandomAccessDataRow(m_rowIndex++);//NOSONAR
        }

        @Override
        public void close() {
            // doesn't close anything because we are not relying on the clients to close their resources
            // instead we close the resources alongside the underlying store

        }
    }

    final class RandomAccessDataRow implements DataRow {

        private final RandomReadAccessRow m_accessRow;

        private final ReadValue[] m_values;

        private final DataCell[] m_cells;

        RandomAccessDataRow(final RandomReadAccessRow accessRow) {
            m_accessRow = accessRow;
            m_values = new ReadValue[accessRow.size()];
            m_cells = new DataCell[accessRow.size()];
        }

        RandomReadAccessRow getAccessRow() {
            return m_accessRow;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return IntStream.range(0, getNumCells()).mapToObj(this::getCell).iterator();
        }

        @Override
        public int getNumCells() {
            return m_values.length - 1;
        }

        @Override
        public RowKey getKey() {
            return m_accessRow.supplyWithInitializedAccess(0, () -> new RowKey(getRowKeyValue().getString()));
        }

        private RowKeyValue getRowKeyValue() {
            return (RowKeyValue)getValue(0);
        }

        private ReadValue getValue(final int index) {
            var value = m_values[index];
            if (value == null) {
                // not synchronized because in the worst case, multiple values are created
                // TODO use UntypedValueFactory once merged
                value = readValue(m_valueSchema.getValueFactory(index), m_accessRow.getAccess(index));
                m_values[index] = value;
            }
            return value;
        }

        @SuppressWarnings("unchecked")
        private <R extends ReadAccess> ReadValue readValue(final ValueFactory<R, ?> valueFactory,
            final ReadAccess access) {
            return valueFactory.createReadValue((R)access);
        }

        @Override
        public DataCell getCell(final int index) {
            var cell = m_cells[index];
            if (cell == null) {
                var valueIndex = index + 1;
                cell = m_accessRow.supplyWithInitializedAccess(valueIndex, () -> getCellFromValue(valueIndex));
                m_cells[index] = cell;
            }
            return cell;
        }

        private DataCell getCellFromValue(final int valueIndex) {
            if (m_accessRow.getAccess(valueIndex).isMissing()) {
                return DataType.getMissingCell();
            } else {
                return getValue(valueIndex).getDataCell();
            }
        }
    }

}
