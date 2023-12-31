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

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.knime.core.columnar.data.NullableReadData;

/**
 * Default implementation of a {@link ReadBatch} that holds an array of {@link NullableReadData} and guarantees that
 * data is present at all valid indices.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class DefaultReadBatch extends AbstractBatch<NullableReadData> implements ReadBatch {

    private final int m_length;

    private final AtomicLong m_size;

    /**
     * @param data the array of data comprising this batch
     */
    public DefaultReadBatch(final NullableReadData[] data) {
        super(data);
        int length = 0;
        for (NullableReadData d : data) {
            length = Math.max(length, d.length());
        }
        m_length = length;
        m_size = new AtomicLong(-1);
    }

    private DefaultReadBatch(final NullableReadData[] data, final DefaultReadBatch toCopy) {
        super(data, toCopy.m_refCounter);
        m_length = toCopy.m_length;
        m_size = new AtomicLong(toCopy.m_size.get());
    }

    @Override
    public ReadBatch decorate(final DataDecorator dataOperator) {
        var transformedDatas = IntStream.range(0, m_data.length)
                .mapToObj(i -> dataOperator.decorate(i, m_data[i]))
                .toArray(NullableReadData[]::new);
        return new DefaultReadBatch(transformedDatas, this);
    }

    @Override
    public boolean isMissing(final int index) {
        return false;
    }

    @Override
    public int length() {
        return m_length;
    }

    @Override
    public final long sizeOf() {
        return m_size.updateAndGet(s -> s == -1 ? super.sizeOf() : s);
    }

    @Override
    public boolean tryRetain() {
        return m_refCounter.tryRetain();
    }

}
