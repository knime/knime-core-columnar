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
 *   Oct 13, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.parallel.exec;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.knime.core.columnar.batch.ReadBatch;

/**
 *
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class RowWriteTask implements AutoCloseable {

    public static final RowWriteTask NULL = new RowWriteTask(new ReadBatch[0], new int[0]);

    private final int[] m_indices;

    private final ReadBatch[] m_batches;

    private RowWriteTask(final ReadBatch[] batches, final int[] indices) {
        m_indices = indices;
        m_batches = batches;
    }

    public int getBatchIndex(final int idx) {
        return m_indices[idx];
    }

    public ReadBatch getBatch(final int idx) {
        return m_batches[idx];
    }

    public int size() {
        return m_indices.length;
    }

    @Override
    public void close() {
        for (var batch : m_batches) {
            batch.release();
        }
    }

    public static Builder builder(final int length) {
        return new Builder(length);
    }

    /**
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class Builder implements AutoCloseable {
        private final int[] m_indices;

        private final ReadBatch[] m_batches;

        private int m_currentIndex = 0;

        Builder(final int length) {
            m_indices = new int[length];
            m_batches = new ReadBatch[length];
        }

        public Builder addPosition(final int index, final ReadBatch batch) {
            batch.retain();
            m_indices[m_currentIndex] = index;
            m_batches[m_currentIndex] = batch;
            m_currentIndex++;
            return this;
        }

        @Override
        public void close() {
            IntStream.range(0, m_currentIndex).forEach(i -> m_batches[i].release());
        }

        public RowWriteTask build() {
            var indices = Arrays.copyOf(m_indices, m_currentIndex);
            var batches = Arrays.copyOf(m_batches, m_currentIndex);
            m_currentIndex = 0;
            return new RowWriteTask(batches, indices);
        }
    }
}