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
package org.knime.core.columnar.batch;

import java.util.Arrays;
import java.util.Objects;

import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class DefaultWriteBatch implements WriteBatch {

    private final ColumnStoreSchema m_schema;

    private final ColumnWriteData[] m_data;

    private final int m_capacity;

    public DefaultWriteBatch(final ColumnStoreSchema schema, final ColumnWriteData[] data, final int capacity) {
        Objects.requireNonNull(schema, () -> "Column store schema must not be null.");
        Objects.requireNonNull(data, () -> "Column data must not be null.");

        m_schema = schema;
        m_data = data;
        m_capacity = capacity;
    }

    @Override
    public ColumnWriteData get(final int colIndex) {
        if (colIndex < 0) {
            throw new IndexOutOfBoundsException(String.format("Column index %d smaller than 0.", colIndex));
        }
        if (colIndex >= m_data.length) {
            throw new IndexOutOfBoundsException(String.format(
                "Column index %d larger then the column store's largest column index (%d).", colIndex, m_data.length-1));
        }
        return m_data[colIndex];
    }

    @Override
    public void release() {
        for (final ColumnWriteData data : m_data) {
            data.release();
        }
    }

    @Override
    public void retain() {
        for (final ColumnWriteData data : m_data) {
            data.retain();
        }
    }

    @Override
    public int sizeOf() {
        return Arrays.stream(m_data).mapToInt(ReferencedData::sizeOf).sum();
    }

    @Override
    public int capacity() {
        return m_capacity;
    }

    @Override
    public ReadBatch close(final int length) {
        final ColumnReadData[] data = new ColumnReadData[m_data.length];
        int capacity = 0;
        for (int i = 0; i < m_data.length; i++) {
            data[i] = m_data[i].close(length);
            capacity = Math.max(capacity, data[i].length());
        }

        return new DefaultReadBatch(m_schema, data, capacity);
    }

//    @Override
//    public void ensureCapacity(final int capacity) {
//        for (final ColumnWriteData data : m_data) {
//            data.ensureCapacity(capacity);
//        }
//        m_capacity = capacity;
//    }
//
//    @Override
//    public int getMaxCapacity() {
//        return m_capacity;
//    }
//
//    @Override
//    public void finishWritingAndTrim(final int numValues) {
//        for (final ColumnWriteData data : m_data) {
//            data.finishWritingAndTrim(numValues);
//        }
//        m_capacity = numValues;
//    }

}
