package org.knime.core.columnar.arrow.onheap.data;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import it.unimi.dsi.fastutil.BigArrays;

class ByteBigArrayHelper {

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
