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
 *   2 Sep 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.cache;

import java.util.Objects;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;

/**
 * An object that uniquely identifies a {@link ReadData} held by a {@link RandomAccessBatchReadable}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ColumnDataUniqueId {

    private final RandomAccessBatchReadable m_readable;

    private final DataIndex m_dataIndex;

    private final int m_batchIndex;

    private final int m_hashCode;

    /**
     * @param readable the readable holding the data
     * @param dataIndex the index of the data (may be nested)
     * @param batchIndex the batch index of the data
     */
    public ColumnDataUniqueId(final RandomAccessBatchReadable readable, final DataIndex dataIndex,
        final int batchIndex) {
        m_readable = readable;
        m_dataIndex = dataIndex;
        m_batchIndex = batchIndex;
        m_hashCode = Objects.hash(m_readable, m_dataIndex, m_batchIndex);
    }

    /**
     * @param readable the readable holding the data
     * @param columnIndex the column index of the data
     * @param batchIndex the batch index of the data
     */
    public ColumnDataUniqueId(final RandomAccessBatchReadable readable, final int columnIndex, final int batchIndex) {
        this(readable, DataIndex.createColumnIndex(columnIndex), batchIndex);
    }

    @Override
    public int hashCode() {
        return m_hashCode;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof ColumnDataUniqueId)
        {
            final ColumnDataUniqueId other = (ColumnDataUniqueId)object;
            return Objects.equals(m_readable, other.m_readable) && m_dataIndex.equals(other.m_dataIndex)
                && m_batchIndex == other.m_batchIndex;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.join(",", m_readable.toString(), m_dataIndex.toString(), Integer.toString(m_batchIndex));
    }

    /**
     * Obtains the {@link RandomAccessBatchReadable} that holds the uniquely identified data.
     *
     * @return the store holding the data
     */
    public RandomAccessBatchReadable getReadable() {
        return m_readable;
    }

    DataIndex getColumnIndex() {
        return m_dataIndex;
    }

    int getBatchIndex() {
        return m_batchIndex;
    }

    /**
     * Provides the child with the given index.
     *
     * @param index of the child
     * @return the child at the given index
     */
    public ColumnDataUniqueId getChild(final int index) {
        return new ColumnDataUniqueId(m_readable, m_dataIndex.getChild(index), m_batchIndex);
    }

    /**
     * Indicates whether this data is on column level or nested e.g. inside of a list or struct.
     * @return true if this corresponds to column level data
     */
    public boolean isColumnLevel() {
        return m_dataIndex.isColumnLevel();
    }

}
