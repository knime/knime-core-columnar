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
package org.knime.core.columnar.filter;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntFunction;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.FilteredReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.store.ColumnStoreSchema;

@SuppressWarnings("javadoc")
public class FilteredColumnSelection implements ColumnSelection {

    private final ColumnStoreSchema m_schema;

    private final int[] m_indices;

    public FilteredColumnSelection(final ColumnStoreSchema schema, final int... indices) {
        Objects.requireNonNull(schema, () -> "Column Store schema must not be null.");
        Objects.requireNonNull(indices, () -> "Indices must not be null.");

        m_schema = schema;
        m_indices = Arrays.stream(indices).sorted().distinct().toArray();
        if (m_indices.length > 0) {
            if (m_indices[0] < 0) {
                throw new IndexOutOfBoundsException(String.format("Column index %d smaller than 0.", m_indices[0]));
            }
            if (m_indices[m_indices.length - 1] > schema.getNumColumns()) {
                throw new IndexOutOfBoundsException(
                    String.format("Column index %d larger then the column store's number of columns (%d).",
                        m_indices[m_indices.length - 1], schema.getNumColumns()));
            }
        }
    }

    public int[] includes() {
        return m_indices;
    }

    @Override
    public ReadBatch createBatch(final IntFunction<ColumnReadData> function) {
        final ColumnReadData[] batch = new ColumnReadData[m_schema.getNumColumns()];
        int length = 0;
        for (int i : includes()) {
            batch[i] = function.apply(i);
            length = Math.max(length, batch[i].length());
        }
        return new FilteredReadBatch(this, new DefaultReadBatch(m_schema, batch, length));
    }

}