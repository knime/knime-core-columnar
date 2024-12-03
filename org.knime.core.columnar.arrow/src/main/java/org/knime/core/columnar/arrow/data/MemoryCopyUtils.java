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
 *   Dec 3, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class MemoryCopyUtils {

    private static final int INT_ARRAY_BASE_OFFSET = MemoryUtil.UNSAFE.arrayBaseOffset(int[].class);

    MemoryCopyUtils() {
    }

    /**
     * Copy the content of the given source address to the given destination array. Fills the destination array from the
     * beginning to the end.
     *
     * @param source the source memory address
     * @param destination the destination int array
     */
    public static void copy(final ArrowBuf source, final int[] destination) {
        copy(source.memoryAddress(), destination);
    }

    /**
     * Copy the content of the given source address to the given destination array. Fills the destination array from the
     * beginning to the end.
     *
     * @param sourceAddress the source memory address
     * @param destination the destination int array
     */
    public static void copy(final long sourceAddress, final int[] destination) {
        MemoryUtil.UNSAFE.copyMemory(//
            null, // source object
            sourceAddress, // source offset
            destination, // destination object
            INT_ARRAY_BASE_OFFSET, // destination offset
            destination.length * Integer.BYTES // number of bytes
        );
    }

    public static void copy(final int[] source, final ArrowBuf destination) {
        copy(source, destination.memoryAddress());
    }

    public static void copy(final int[] source, final long destinationAddress) {
        MemoryUtil.UNSAFE.copyMemory(//
            source, // source object
            INT_ARRAY_BASE_OFFSET, // source offset
            null, // destination object
            destinationAddress, // destination offset
            source.length * Integer.BYTES // number of bytes
        );
    }
}
