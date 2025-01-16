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
package org.knime.core.columnar.arrow.unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;

/**
 * Provides low-level memory operations using the {@link sun.misc.Unsafe} API. This class enables:
 * <ul>
 * <li>Copying memory directly between Java objects.</li>
 * <li>Invoking the cleaner on direct {@link ByteBuffer} instances to force deallocation.</li>
 * </ul>
 * <p>
 * <strong>Usage Note:</strong> The {@code sun.misc.Unsafe} API is internal and not guaranteed to be available or stable
 * across different JVM versions. Improper use may cause JVM crashes or data corruption. Furthermore, accessing it via
 * reflection requires specific JVM command-line flags (e.g., {@code --add-opens=java.base/java.nio=ALL-UNNAMED} or
 * similar). Use with caution and only when absolutely necessary for performance-critical applications.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class UnsafeUtil {

    private static final Unsafe UNSAFE;

    /** The offset of the first element in a byte array. */
    public static final int BYTE_ARRAY_BASE_OFFSET;

    /** The offset of the first element in an int array. */
    public static final int INT_ARRAY_BASE_OFFSET;

    /** The offset of the first element in a long array. */
    public static final int LONG_ARRAY_BASE_OFFSET;

    /** The offset of the first element in a float array. */
    public static final int FLOAT_ARRAY_BASE_OFFSET;

    /** The offset of the first element in a double array. */
    public static final int DOUBLE_ARRAY_BASE_OFFSET;

    // copied from org.apache.arrow.memory.MemoryUtil (their UNSAFE field was made private in 18.0)
    static {
        try {
            // try to get the unsafe object
            final Object maybeUnsafe = AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    try {
                        final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                        unsafeField.setAccessible(true);
                        return unsafeField.get(null);
                    } catch (Throwable e) {
                        return e;
                    }
                }
            });

            if (maybeUnsafe instanceof Throwable) {
                throw (Throwable)maybeUnsafe;
            }

            UNSAFE = (Unsafe)maybeUnsafe;
            BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            INT_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
            LONG_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
            FLOAT_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
            DOUBLE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
        } catch (Throwable e) {
            // This exception will get swallowed, but it's necessary for the static analysis that ensures
            // the static fields above get initialized
            final RuntimeException failure =
                new RuntimeException("Failed to initialize MemoryUtil. You must start Java with "
                    + "`--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` "
                    + "(See https://arrow.apache.org/docs/java/install.html)", e);
            failure.printStackTrace();
            throw failure;
        }
    }

    private UnsafeUtil() {
    }

    /**
     * @param directBuffer
     * @see Unsafe#invokeCleaner(ByteBuffer)
     */
    public static void invokeCleaner(final ByteBuffer directBuffer) {
        UNSAFE.invokeCleaner(directBuffer);
    }

    /**
     * @param srcBase
     * @param srcOffset
     * @param destBase
     * @param destOffset
     * @param bytes
     * @see Unsafe#copyMemory(Object, long, Object, long, long)
     */
    public static void copyMemory( //
        final Object srcBase, //
        final long srcOffset, //
        final Object destBase, //
        final long destOffset, //
        final long bytes //
    ) {
        UNSAFE.copyMemory(srcBase, srcOffset, destBase, destOffset, bytes);
    }
}
