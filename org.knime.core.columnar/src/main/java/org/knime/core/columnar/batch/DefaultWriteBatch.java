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

import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Default implementation of a {@link WriteBatch} that holds an array of {@link NullableWriteData}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class DefaultWriteBatch implements WriteBatch {

    private final NullableWriteData[] m_data;

    private int m_capacity;

    /**
     * @param data the array of data backing this batch
     */
    public DefaultWriteBatch(final NullableWriteData[] data) {
        m_data = data;
        m_capacity = Arrays.stream(data).mapToInt(WriteData::capacity).min().orElse(0);
    }

    @Override
    public NullableWriteData get(final int index) {
        return m_data[index];
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
    public int capacity() {
        return m_capacity;
    }

    @Override
    public void expand(final int minimumCapacity) {
        int newCapacity = Integer.MAX_VALUE;
        for (final NullableWriteData data : m_data) {
            data.expand(minimumCapacity);
            newCapacity = Math.min(newCapacity, data.capacity());
        }
        m_capacity = newCapacity;
    }

    @Override
    public ReadBatch close(final int length) {
        return new DefaultReadBatch(Arrays.stream(m_data).map(d -> d.close(length)).toArray(NullableReadData[]::new));
    }

}
