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
 *   20 Jun 2025 (pietzsch): created
 */
package org.knime.core.columnar.arrow.onheap.data;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.bytes.ByteBigArrays;

/**
 * Utility methods to read multi-byte primitives ({@code long}, {@code float}, ...) in LITTLE_ENDIAN ordering from
 * arbitrary offsets in a {@code byte[][]} {@link ByteBigArrays big array}.
 *
 * @author Tobias Pietzsch
 */
final class ByteBigArrayHelper {

    private ByteBigArrayHelper() {
        // should not be instantiated
    }

    static short getShort(final byte[][] a, final long index) {
        final int displacement = BigArrays.displacement(index);
        if (displacement >= BigArrays.SEGMENT_SIZE - 2) {
            return getTornShort(a, index);
        } else {
            final int segment = BigArrays.segment(index);
            return (short)SHORT.get(a[segment], displacement);
        }
    }

    private static short getTornShort(final byte[][] a, final long index) {
        final byte[] buf = new byte[2];
        BigArrays.copyFromBig(a, index, buf, 0, 2);
        return (short)SHORT.get(buf, 0);
    }

    static int getInt(final byte[][] a, final long index) {
        final int displacement = BigArrays.displacement(index);
        if (displacement >= BigArrays.SEGMENT_SIZE - 4) {
            return getTornInt(a, index);
        } else {
            final int segment = BigArrays.segment(index);
            return (int)INT.get(a[segment], displacement);
        }
    }

    private static int getTornInt(final byte[][] a, final long index) {
        final byte[] buf = new byte[4];
        BigArrays.copyFromBig(a, index, buf, 0, 4);
        return (int)INT.get(buf, 0);
    }

    static long getLong(final byte[][] a, final long index) {
        final int displacement = BigArrays.displacement(index);
        if (displacement >= BigArrays.SEGMENT_SIZE - 8) {
            return getTornLong(a, index);
        } else {
            final int segment = BigArrays.segment(index);
            return (long)LONG.get(a[segment], displacement);
        }
    }

    private static long getTornLong(final byte[][] a, final long index) {
        final byte[] buf = new byte[8];
        BigArrays.copyFromBig(a, index, buf, 0, 8);
        return (long)LONG.get(buf, 0);
    }

    static float getFloat(final byte[][] a, final long index) {
        final int displacement = BigArrays.displacement(index);
        if (displacement >= BigArrays.SEGMENT_SIZE - 4) {
            return getTornFloat(a, index);
        } else {
            final int segment = BigArrays.segment(index);
            return (float)FLOAT.get(a[segment], displacement);
        }
    }

    private static float getTornFloat(final byte[][] a, final long index) {
        final byte[] buf = new byte[4];
        BigArrays.copyFromBig(a, index, buf, 0, 4);
        return (float)FLOAT.get(buf, 0);
    }

    static double getDouble(final byte[][] a, final long index) {
        final int displacement = BigArrays.displacement(index);
        if (displacement >= BigArrays.SEGMENT_SIZE - 8) {
            return getTornDouble(a, index);
        } else {
            final int segment = BigArrays.segment(index);
            return (double)DOUBLE.get(a[segment], displacement);
        }
    }

    private static double getTornDouble(final byte[][] a, final long index) {
        final byte[] buf = new byte[8];
        BigArrays.copyFromBig(a, index, buf, 0, 8);
        return (double)DOUBLE.get(buf, 0);
    }

    private static final VarHandle SHORT = create(short[].class);
    private static final VarHandle INT = create(int[].class);
    private static final VarHandle FLOAT = create(float[].class);
    private static final VarHandle LONG = create(long[].class);
    private static final VarHandle DOUBLE = create(double[].class);

    private static VarHandle create(final Class<?> viewArrayClass) {
        return MethodHandles.byteArrayViewVarHandle(viewArrayClass, ByteOrder.LITTLE_ENDIAN);
    }
}
