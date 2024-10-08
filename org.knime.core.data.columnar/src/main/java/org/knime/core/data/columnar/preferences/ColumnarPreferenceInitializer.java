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
 *   2 Oct 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.preferences;

import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.COLUMNAR_STORE;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.knime.core.node.NodeLogger;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ColumnarPreferenceInitializer extends AbstractPreferenceInitializer {

    static final String OFF_HEAP_MEM_LIMIT_KEY = "knime.core.data.columnar.off-heap-limit";

    static final int OFF_HEAP_MEM_LIMIT_MIN = 768;

    static final int OFF_HEAP_MEM_LIMIT_DEF = computeDefaultOffHeapMemoryLimit();

    /**
     * The amount of physical memory reserved for the system. The default off-heap memory will leave at least this much
     * memory available.
     */
    private static final int SYS_RESERVED_MEM_MB = 1024;

    /**
     * A factor that is multiplied with the configured heap and off-heap memory to get the expected memory usage of the
     * application.
     */
    private static final double JVM_FACTOR = 1.3;

    @Override
    public void initializeDefaultPreferences() {
        COLUMNAR_STORE.setDefault(OFF_HEAP_MEM_LIMIT_KEY, OFF_HEAP_MEM_LIMIT_DEF);
    }

    /**
     * Compute the default off-heap memory. Uses whichever is less but at least {@link #OFF_HEAP_MEM_LIMIT_MIN}.
     * <ul>
     * <li>the amount of memory as used for the heap</li>
     * <li>the amount of off-heap memory that fulfills the following formula
     *
     * <pre>
     * (heapLimitMb + offHeapLimitMb) * {@link #JVM_FACTOR} + {@link #SYS_RESERVED_MEM_MB} = physicalMemoryMb
     * </pre>
     *
     * </li>
     * </ul>
     */
    private static int computeDefaultOffHeapMemoryLimit() {
        var heapLimitMb = (int)(ColumnarPreferenceUtils.getMaxHeapSize() >> 20);
        var physicalMemoryMb = (int)(ColumnarPreferenceUtils.getTotalPhysicalMemorySize() >> 20);

        var offHeapLimitMb = Math.min( //
            heapLimitMb, //
            (int)((physicalMemoryMb - SYS_RESERVED_MEM_MB) / JVM_FACTOR - heapLimitMb) //
        );

        if (offHeapLimitMb < OFF_HEAP_MEM_LIMIT_MIN) {
            NodeLogger.getLogger(ColumnarPreferenceInitializer.class).warn(
                "Configured heap memory limit does not leave enough free memory for the columnar backend off-heap memory. "
                    + "Using " + OFF_HEAP_MEM_LIMIT_MIN + "Mb for the off-heap memory limit.");
            return OFF_HEAP_MEM_LIMIT_MIN;
        }
        return offHeapLimitMb;
    }
}
