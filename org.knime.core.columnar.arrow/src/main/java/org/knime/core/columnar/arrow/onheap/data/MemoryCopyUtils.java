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
package org.knime.core.columnar.arrow.onheap.data;

import static org.knime.core.columnar.arrow.unsafe.UnsafeUtil.DOUBLE_ARRAY_BASE_OFFSET;
import static org.knime.core.columnar.arrow.unsafe.UnsafeUtil.FLOAT_ARRAY_BASE_OFFSET;
import static org.knime.core.columnar.arrow.unsafe.UnsafeUtil.INT_ARRAY_BASE_OFFSET;
import static org.knime.core.columnar.arrow.unsafe.UnsafeUtil.LONG_ARRAY_BASE_OFFSET;

import org.apache.arrow.memory.ArrowBuf;
import org.knime.core.columnar.arrow.unsafe.UnsafeUtil;

import it.unimi.dsi.fastutil.BigArrays;

/**
 * Utility class for copying memory contents between ArrowBufs and primitive arrays using fast low-level memory access.
 *
 * Methods assume that:
 * <ul>
 * <li>The entire array should be copied (from start to end).</li>
 * <li>The ArrowBuf is large enough to hold the entire array being copied.</li>
 * <li>The calls are trusted, and no additional bounds checks (beyond what Unsafe might enforce) are performed.</li>
 * </ul>
 *
 * Use with caution.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class MemoryCopyUtils {

    private MemoryCopyUtils() {
    }

    // ------------------------------------------------------------------------
    // int[] copy methods
    // ------------------------------------------------------------------------

    /**
     * Copy the content of the given source ArrowBuf into the given int array.
     *
     * @param source the source ArrowBuf
     * @param destination the destination int array
     */
    public static void copy(final ArrowBuf source, final int[] destination) {
        copy(source.memoryAddress(), destination);
    }

    /**
     * Copy the content of the given source address to the given int array.
     *
     * @param sourceAddress the source memory address
     * @param destination the destination int array
     */
    public static void copy(final long sourceAddress, final int[] destination) {
        UnsafeUtil.copyMemory( //
            null, // source object
            sourceAddress, // source offset
            destination, // destination object
            INT_ARRAY_BASE_OFFSET, // destination offset
            (long)destination.length * Integer.BYTES // number of bytes
        );
    }

    /**
     * Copy the content of the given int array into the given ArrowBuf.
     *
     * @param source the source int array
     * @param destination the destination ArrowBuf
     */
    public static void copy(final int[] source, final ArrowBuf destination) {
        copy(source, destination.memoryAddress());
    }

    /**
     * Copy the content of the given int array into the given destination address.
     *
     * @param source the source int array
     * @param destinationAddress the destination memory address
     */
    public static void copy(final int[] source, final long destinationAddress) {
        UnsafeUtil.copyMemory( //
            source, // source object
            INT_ARRAY_BASE_OFFSET, // source offset
            null, // destination object
            destinationAddress, // destination offset
            (long)source.length * Integer.BYTES // number of bytes
        );
    }

    // ------------------------------------------------------------------------
    // long[] copy methods
    // ------------------------------------------------------------------------

    /**
     * Copy the content of the given source ArrowBuf into the given long array.
     *
     * @param source the source ArrowBuf
     * @param destination the destination long array
     */
    public static void copy(final ArrowBuf source, final long[] destination) {
        copy(source.memoryAddress(), destination);
    }

    /**
     * Copy the content of the given source address to the given long array.
     *
     * @param sourceAddress the source memory address
     * @param destination the destination long array
     */
    public static void copy(final long sourceAddress, final long[] destination) {
        UnsafeUtil.copyMemory( //
            null, // source object
            sourceAddress, // source offset
            destination, // destination object
            LONG_ARRAY_BASE_OFFSET, // destination offset
            (long)destination.length * Long.BYTES // number of bytes
        );
    }

    /**
     * Copy the content of the given long array into the given ArrowBuf.
     *
     * @param source the source long array
     * @param destination the destination ArrowBuf
     */
    public static void copy(final long[] source, final ArrowBuf destination) {
        copy(source, destination.memoryAddress());
    }

    /**
     * Copy the content of the given long array into the given destination address.
     *
     * @param source the source long array
     * @param destinationAddress the destination memory address
     */
    public static void copy(final long[] source, final long destinationAddress) {
        UnsafeUtil.copyMemory( //
            source, // source object
            LONG_ARRAY_BASE_OFFSET, // source offset
            null, // destination object
            destinationAddress, // destination offset
            (long)source.length * Long.BYTES // number of bytes
        );
    }

    // ------------------------------------------------------------------------
    // float[] copy methods
    // ------------------------------------------------------------------------

    /**
     * Copy the content of the given source ArrowBuf into the given float array.
     *
     * @param source the source ArrowBuf
     * @param destination the destination float array
     */
    public static void copy(final ArrowBuf source, final float[] destination) {
        copy(source.memoryAddress(), destination);
    }

    /**
     * Copy the content of the given source address to the given float array.
     *
     * @param sourceAddress the source memory address
     * @param destination the destination float array
     */
    public static void copy(final long sourceAddress, final float[] destination) {
        UnsafeUtil.copyMemory( //
            null, // source object
            sourceAddress, // source offset
            destination, // destination object
            FLOAT_ARRAY_BASE_OFFSET, // destination offset
            (long)destination.length * Float.BYTES // number of bytes
        );
    }

    /**
     * Copy the content of the given float array into the given ArrowBuf.
     *
     * @param source the source float array
     * @param destination the destination ArrowBuf
     */
    public static void copy(final float[] source, final ArrowBuf destination) {
        copy(source, destination.memoryAddress());
    }

    /**
     * Copy the content of the given float array into the given destination address.
     *
     * @param source the source float array
     * @param destinationAddress the destination memory address
     */
    public static void copy(final float[] source, final long destinationAddress) {
        UnsafeUtil.copyMemory( //
            source, // source object
            FLOAT_ARRAY_BASE_OFFSET, // source offset
            null, // destination object
            destinationAddress, // destination offset
            (long)source.length * Float.BYTES // number of bytes
        );
    }

    // ------------------------------------------------------------------------
    // double[] copy methods
    // ------------------------------------------------------------------------

    /**
     * Copy the content of the given source ArrowBuf into the given double array.
     *
     * @param source the source ArrowBuf
     * @param destination the destination double array
     */
    public static void copy(final ArrowBuf source, final double[] destination) {
        copy(source.memoryAddress(), destination);
    }

    /**
     * Copy the content of the given source address to the given double array.
     *
     * @param sourceAddress the source memory address
     * @param destination the destination double array
     */
    public static void copy(final long sourceAddress, final double[] destination) {
        UnsafeUtil.copyMemory( //
            null, // source object
            sourceAddress, // source offset
            destination, // destination object
            DOUBLE_ARRAY_BASE_OFFSET, // destination offset
            (long)destination.length * Double.BYTES // number of bytes
        );
    }

    /**
     * Copy the content of the given double array into the given ArrowBuf.
     *
     * @param source the source double array
     * @param destination the destination ArrowBuf
     */
    public static void copy(final double[] source, final ArrowBuf destination) {
        copy(source, destination.memoryAddress());
    }

    /**
     * Copy the content of the given double array into the given destination address.
     *
     * @param source the source double array
     * @param destinationAddress the destination memory address
     */
    public static void copy(final double[] source, final long destinationAddress) {
        UnsafeUtil.copyMemory( //
            source, // source object
            DOUBLE_ARRAY_BASE_OFFSET, // source offset
            null, // destination object
            destinationAddress, // destination offset
            (long)source.length * Double.BYTES // number of bytes
        );
    }

    // ------------------------------------------------------------------------
    // byte big arrays
    // ------------------------------------------------------------------------

    /**
     * Copy the content of the given source ArrowBuf into the given {@link BigArrays big byte array}.
     *
     * @param source the source ArrowBuf
     * @param destination the destination big byte array
     */
    public static void copy(final ArrowBuf source, final byte[][] destination) {
        long offset = 0;
        for (int i = 0; i < destination.length; i++) {
            source.getBytes(offset, destination[i]);
            offset += destination[i].length;
        }
    }

    /**
     * Copy the content of the given {@link BigArrays big byte array} into the given ArrowBuf.
     *
     * @param source the source big byte array
     * @param destination the destination ArrowBuf
     */
    public static void copy(final byte[][] source, final ArrowBuf destination) {
        long offset = 0;
        for (int i = 0; i < source.length; i++) {
            destination.setBytes(offset, source[i]);
            offset += source[i].length;
        }
    }
}
