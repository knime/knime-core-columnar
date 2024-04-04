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
 */
package org.knime.core.columnar;

/**
 * {@link ReferencedData} that can be written up to a certain capacity.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public interface WriteData extends ReferencedData {

    // TODO do we still need expand and capacity?

    /**
     * Expand the data, potentially increasing capacity. Note that the capacity to which the data will be expanded might
     * be larger than the provided minimum capacity.
     *
     * @param minimumCapacity the capacity to which the data shall be expanded at minimum
     */
    void expand(int minimumCapacity);

    /**
     * The non-negative capacity of the data, i.e., the number of elements that can be written.
     *
     * @return capacity, i.e., maximum number of elements
     */
    int capacity();

    /**
     * Close and dispose of this writable data, creating a readable data of a certain length. The data may only be
     * closed if its reference count is currently at one. After closing the data, no other operations are allowed on the
     * {@link WriteData}.
     *
     * @param length the length of the to-be-created {@link ReadData}, i.e., the number of elements that have been
     *            written and that can be read.
     * @return a new {@link ReadData} from which the written data can be read
     * @throws IllegalStateException when the reference count is unequal to one
     */
    ReadData close(int length);

    /**
     * The memory footprint used by the first <code>numElements</code> elements. Closing the data ({@link #close(int)})
     * with the same length will result in an {@link ReadData} object with the same memory footprint (e.g.
     * {@link ReadData#sizeOf()}).
     *
     * TODO - Is this true???
     *
     * @param numElements
     * @return the memory footprint in bytes
     */
    long usedSizeFor(int numElements);
}
