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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.knime.core.columnar.ReferencedData;
import org.knime.core.columnar.data.ColumnData;
import org.knime.core.columnar.filter.FilteredColumnSelection;

/**
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class FilteredBatch implements Batch {

    private final Set<Integer> m_indices;

    private final Batch m_delegate;

    public FilteredBatch(final FilteredColumnSelection selection, final Batch delegate) {
        Objects.requireNonNull(selection, () -> "Column selection must not be null.");
        Objects.requireNonNull(delegate, () -> "Delegate batch must not be null.");

        m_indices = Arrays.stream(selection.includes()).boxed().collect(Collectors.toSet());
        m_delegate = delegate;
    }

    @Override
    public ColumnData get(final int colIndex) {
        if (!m_indices.contains(colIndex)) {
            throw new NoSuchElementException(
                String.format("Data at index %d is not available in this filtered batch.", colIndex));
        }
        return m_delegate.get(colIndex);
    }

    @Override
    public void release() {
        for (int index : m_indices) {
            m_delegate.get(index).release();
        }
    }

    @Override
    public void retain() {
        for (int index : m_indices) {
            m_delegate.get(index).retain();
        }
    }

    @Override
    public int sizeOf() {
        return m_indices.stream().map(m_delegate::get).mapToInt(ReferencedData::sizeOf).sum();
    }

    @Override
    public int length() {
        return m_delegate.length();
    }

}
