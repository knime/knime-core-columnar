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

import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Default implementation of a {@link WriteBatch} that holds an array of {@link NullableWriteData}.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class DefaultWriteBatch extends AbstractBatch<NullableWriteData> implements WriteBatch {

    private int m_capacity;

    /**
     * @param data the array of data comprising this batch
     */
    public DefaultWriteBatch(final NullableWriteData[] data) {
        super(data);
        m_capacity = Arrays.stream(data).mapToInt(WriteData::capacity).min().orElse(0);
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
        // The WriteData contract states that: "The data may only be closed if
        // its reference count is currently at one."
        final boolean released = m_refCounter.release();
        assert released : "Trying to close a WriteBatch with outstanding references.";

        final NullableReadData[] datas = Arrays.stream(m_data)//
                .map(d -> d.close(length))//
                .toArray(NullableReadData[]::new);

        // The WriteData contract states that: "After closing the data, no other
        // operations are allowed on the WriteData." Setting m_data[i] to null
        // should prevent (at least some of these) forbidden operations.
        Arrays.fill(m_data, null);

        return new DefaultReadBatch(datas);
    }

    @Override
    public long usedSizeFor(final int numElements) {
        var totalSize = 0L;
        for (var d: m_data) {
            totalSize += d.usedSizeFor(numElements);
        }
        return totalSize;
    }
}
