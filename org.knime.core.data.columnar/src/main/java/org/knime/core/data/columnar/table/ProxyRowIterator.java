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
import java.util.List;
import java.util.NoSuchElementException;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;

/**
 * A RowIterator that stores the current position in the table and uses proxies to access
 * the actual DataCells.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ProxyRowIterator extends CloseableRowIterator {

    private final ProxyValue<RowKey> m_keyProxy;

    private final List<ProxyValue<DataCell>> m_cellProxies;

    private final long m_numRows;

    private long m_rowIndex;

    ProxyRowIterator(final ProxyValue<RowKey> keyProxy, final List<ProxyValue<DataCell>> cellProxies, final long numRows) {
        m_keyProxy = keyProxy;
        m_cellProxies = cellProxies;
        m_numRows = numRows;
    }

    @Override
    public boolean hasNext() {
        return m_rowIndex < m_numRows;
    }

    @Override
    public DataRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return new ProxyRow(m_rowIndex++, m_keyProxy, m_cellProxies);//NOSONAR
    }

    @Override
    public void close() {
        // doesn't close anything because we are not relying on the clients to close their resources
        // instead we close the resources alongside the underlying store

    }

    private static final class ProxyRow implements DataRow {

        private final long m_rowIndex;

        private final List<ProxyValue<DataCell>> m_cellProxies;

        private final ProxyValue<RowKey> m_keyProxy;

        ProxyRow(final long rowIndex, final ProxyValue<RowKey> keyProxy, final List<ProxyValue<DataCell>> cellProxies) {
            m_rowIndex = rowIndex;
            m_cellProxies = cellProxies;
            m_keyProxy = keyProxy;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return m_cellProxies.stream()//
                    .map(p -> p.getValue(m_rowIndex))//
                    .iterator();
        }

        @Override
        public int getNumCells() {
            return m_cellProxies.size();
        }

        @Override
        public RowKey getKey() {
            return m_keyProxy.getValue(m_rowIndex);
        }

        @Override
        public DataCell getCell(final int index) {
            return m_cellProxies.get(index).getValue(m_rowIndex);
        }

    }

    interface ProxyValue<T> {
        T getValue(long rowIndex);
    }
}
