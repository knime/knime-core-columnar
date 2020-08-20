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
 */
package org.knime.core.columnar.table;

import java.io.IOException;

import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataWriter;

public final class TableWriteCursor implements AutoCloseable {

    private final ColumnDataWriter m_writer;

    private final ColumnDataFactory m_factory;

    private final ColumnDataAccess<ColumnData>[] m_access;

    private ColumnData[] m_currentData;

    private long m_currentDataMaxIndex;

    private int m_index = -1;

    TableWriteCursor(final ColumnDataFactory factory, final ColumnDataWriter writer,
        final ColumnDataAccess<ColumnData>[] access) {
        m_writer = writer;
        m_factory = factory;
        m_access = access;

        switchToNextData();
    }

    public void fwd() {
        if (++m_index > m_currentDataMaxIndex) {
            switchToNextData();
            m_index = 0;
        }
        for (int i = 0; i < m_access.length; i++) {
            m_access[i].fwd();
        }
    }

    public <W extends WriteValue> W get(final int index) {
        @SuppressWarnings("unchecked")
        final W value = (W)m_access[index];
        return value;
    }

    @Override
    public void close() throws Exception {
        releaseCurrentData(m_index + 1);
        m_writer.close();
    }

    private void switchToNextData() {
        releaseCurrentData(m_index);
        m_currentData = m_factory.create();
        for (int i = 0; i < m_access.length; i++) {
            m_access[i].load(m_currentData[i]);
        }
        m_currentDataMaxIndex = m_currentData[0].getMaxCapacity() - 1;
    }

    private void releaseCurrentData(final int numValues) {
        if (m_currentData != null) {
            for (final ColumnData data : m_currentData) {
                data.setNumValues(numValues);
            }
            try {
                m_writer.write(m_currentData);
            } catch (IOException e) {
                throw new RuntimeException("Problem occured when writing column chunk.", e);
            }
            for (final ColumnData data : m_currentData) {
                data.release();
            }
            m_currentData = null;
        }
    }
}