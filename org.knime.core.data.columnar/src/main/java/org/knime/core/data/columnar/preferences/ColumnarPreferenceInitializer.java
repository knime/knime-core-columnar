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
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.HeapCache;

@SuppressWarnings("javadoc")
public class ColumnarPreferenceInitializer extends AbstractPreferenceInitializer {

    static final String USE_DEFAULTS_KEY = "knime.core.data.columnar.use-defaults";

    static final boolean USE_DEFAULTS_DEF = true;

    static final String NUM_THREADS_KEY = "knime.core.data.columnar.num-threads";

    static final int NUM_THREADS_DEF = Math.max(1, ColumnarPreferenceUtils.getNumAvailableProcessors() / 2);

    // the size (in MB) of the LRU cache for entire small tables
    static final String SMALL_TABLE_CACHE_SIZE_KEY = "knime.core.data.columnar.small-cache-size";

    static final int SMALL_TABLE_CACHE_SIZE_DEF = 32;

    // the size (in MB) up to which a table is considered small
    static final String SMALL_TABLE_THRESHOLD_KEY = "knime.core.data.columnar.small-threshold";

    static final int SMALL_TABLE_THRESHOLD_DEF = 1;

    static final String HEAP_CACHE_NAME_KEY = "knime.core.data.columnar.heap-cache";

    static final String HEAP_CACHE_NAME_DEF = HeapCache.WEAK.name();

    // the size (in MB) of the LRU cache for ColumnData of all tables
    static final String COLUMN_DATA_CACHE_SIZE_KEY = "knime.core.data.columnar.data-cache-size";

    static final int COLUMN_DATA_CACHE_SIZE_DEF = Math.max(0, Math.min(
        (int)(ColumnarPreferenceUtils.getMaxHeapSize() >> 20),
        ColumnarPreferenceUtils.getUsablePhysicalMemorySizeMB() - ColumnarPreferenceUtils.getSmallTableCacheSize()));

    @Override
    public void initializeDefaultPreferences() {
        COLUMNAR_STORE.setDefault(USE_DEFAULTS_KEY, USE_DEFAULTS_DEF);
        COLUMNAR_STORE.setDefault(NUM_THREADS_KEY, NUM_THREADS_DEF);
        COLUMNAR_STORE.setDefault(HEAP_CACHE_NAME_KEY, HEAP_CACHE_NAME_DEF);
        COLUMNAR_STORE.setDefault(SMALL_TABLE_CACHE_SIZE_KEY, SMALL_TABLE_CACHE_SIZE_DEF);
        COLUMNAR_STORE.setDefault(SMALL_TABLE_THRESHOLD_KEY, SMALL_TABLE_THRESHOLD_DEF);
        COLUMNAR_STORE.setDefault(COLUMN_DATA_CACHE_SIZE_KEY, COLUMN_DATA_CACHE_SIZE_DEF);
    }

}
