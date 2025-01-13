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
 *   Jan 16, 2025 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap;

import java.util.Arrays;

import org.knime.core.columnar.arrow.onheap.data.ValidityBuffer;

import com.google.common.base.Preconditions;

/**
 * A class holding the null count for a vector and its children.
 * <p>
 * TODO(AP-23856) The nullCount is unused by the on-heap implementation. It should be used to optimize the performance
 * of `#isMissing` by checking if all values are missing or no values are missing. See AbstractOffHeapArrowReadData. A
 * new implementation of this optimization could live in the {@link ValidityBuffer}. If this optimization has no
 * benefit, the nullCount should be removed.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class ArrowVectorNullCount {

    private final int m_nullCount;

    private final ArrowVectorNullCount[] m_children;

    /**
     * Create a {@link ArrowVectorNullCount} with the given null count for the vector and its children.
     *
     * @param nullCount the null count of the vector
     * @param children the {@link ArrowVectorNullCount null counts} for the child vectors
     */
    ArrowVectorNullCount(final int nullCount, final ArrowVectorNullCount[] children) {
        Preconditions.checkNotNull(children);
        m_nullCount = nullCount;
        m_children = children;
    }

    /** @return the null count for the vector */
    public int getNullCount() {
        return m_nullCount;
    }

    /**
     * @param index the index of the child
     * @return the null count for the child vector
     */
    public ArrowVectorNullCount getChild(final int index) {
        return m_children[index];
    }

    @Override
    public String toString() {
        return "ArrowVectorNullCount [m_nullCount=" + m_nullCount + ", m_children=" + Arrays.toString(m_children) + "]";
    }
}