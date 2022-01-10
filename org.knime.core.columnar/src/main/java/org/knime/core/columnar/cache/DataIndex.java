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
 *   Nov 29, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

/**
 * Identifies a data object within a nested structure.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DataIndex {

    private final int m_index;

    private final DataIndex m_parent;

    private final int m_hashCode;

    /**
     * Creates the root index for a column.
     *
     * @param columnIndex index of the column within the table
     * @return an index identifying the root level of the column
     */
    public static DataIndex createColumnIndex(final int columnIndex) {
        // TODO cache DataIndex objects if object creation proofs to be a performance issue
        return new DataIndex(columnIndex);
    }

    private DataIndex(final int columnIndex) {
        this(null, columnIndex);
    }

    private DataIndex(final DataIndex parent, final int index) {
        Preconditions.checkArgument(index >= 0, "Negative indices are not permitted.");
        m_parent = parent;
        m_index = index;
        m_hashCode = Objects.hash(parent, index);
    }

    /**
     * Creates a child index.
     *
     * @param childIndex index of the child
     * @return an index identifying the child at the provided index
     */
    public DataIndex getChild(final int childIndex) {
        return new DataIndex(this, childIndex);
    }

    /**
     * @return true if this index is on column level i.e. it has no parent
     */
    public boolean isColumnLevel() {
        return m_parent == null;
    }

    @Override
    public int hashCode() {
        return m_hashCode;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof DataIndex) {
            var other = (DataIndex)obj;
            return m_hashCode == other.m_hashCode//
                    && m_index == other.m_index//
                    && Objects.equals(m_parent, other.m_parent);
        } else {
            return false;
        }
    }

    private IntStream indexStream() {
        if (m_parent == null) {
            return IntStream.of(m_index);
        } else {
            return IntStream.concat(m_parent.indexStream(), IntStream.of(m_index));
        }
    }

    @Override
    public String toString() {
        return indexStream()//
                .mapToObj(Integer::toString)//
                .collect(Collectors.joining(",", "[", "]"));
    }
}
