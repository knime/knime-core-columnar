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
 *   15 Sep 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.batch;

import java.util.Arrays;

import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Default implementation of a {@link ReadBatch} that holds an array of {@link NullableReadData} and guarantees that
 * data is present at all valid indices.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class DefaultReadBatch implements ReadBatch {

    private final NullableReadData[] m_data;

    private final int m_length;

    /**
     * Creates a new batch of data.
     *
     * @param data the non-null array of nullable data that comprises this batch
     */
    public DefaultReadBatch(final NullableReadData[] data) {
        int length = 0;
        for (NullableReadData d : data) {
            length = Math.max(length, d.length());
        }
        m_data = data;
        m_length = length;
    }

    /**
     * Obtains the {@link NullableReadData} at a certain index.
     *
     * @param index the index at which to look for the data
     * @return the non-null data at the given index
     * @throws IndexOutOfBoundsException if the index is negative or equal to or greater than the size of the batch
     */
    @Override
    public NullableReadData get(final int index) {
        return m_data[index];
    }

    /**
     * Obtains an array of all {@link NullableReadData} in this batch. This implementation of the method is unsafe,
     * since the array it returns is the array underlying the batch (and not a defensive copy thereof). Clients must not
     * modify the returned array.
     *
     * @return the non-null array of all data in this batch
     */
    @Override
    public NullableReadData[] getUnsafe() {
        return m_data;
    }

    @Override
    public int length() {
        return m_length;
    }

    @Override
    public void release() {
        Arrays.stream(m_data).forEach(ReferencedData::release);
    }

    @Override
    public void retain() {
        Arrays.stream(m_data).forEach(ReferencedData::retain);
    }

    @Override
    public long sizeOf() {
        return Arrays.stream(m_data).mapToLong(ReferencedData::sizeOf).sum();
    }

    @Override
    public int size() {
        return m_data.length;
    }

}
