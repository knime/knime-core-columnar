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
 * Data that tracks how many clients reference it. When created, the data is referenced once, by the client that created
 * it. Whenever an additional client requests access on the data, the reference count must be increased by invoking
 * {@link ReferencedData#retain() retain}. Whenever a client no longer needs access on the data, the reference count
 * must be decreased by invoking {@link ReferencedData#release() release}. When the reference count reaches zero, the
 * data is discarded and any underlying resources are relinquished.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public interface ReferencedData {

    /**
     * Retain data, increasing reference count by one.
     *
     * @throws IllegalStateException when the data has already been discarded as a consequence of the reference count
     *             reaching zero
     */
    void retain();

    /**
     * Release data, decreasing reference count by one. When the reference count reaches zero, the data is discarded and
     * any underlying resources are relinquished.
     */
    void release();

    /**
     * Determine the memory footprint of the data in bytes or a pessimistic estimate thereof.
     *
     * @return size of data in bytes
     */
    long sizeOf();

}
