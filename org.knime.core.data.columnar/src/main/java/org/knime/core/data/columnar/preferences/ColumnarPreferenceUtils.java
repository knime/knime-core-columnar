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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.ui.preferences.ScopedPreferenceStore;
import org.knime.core.columnar.cache.AsyncFlushCachedColumnStore;
import org.knime.core.columnar.cache.CachedColumnReadStore;
import org.knime.core.columnar.cache.CachedColumnStoreCache;
import org.knime.core.columnar.cache.SmallColumnStore;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.phantom.PhantomReferenceReadStore;
import org.knime.core.columnar.phantom.PhantomReferenceStore;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.data.columnar.ColumnarTableBackend;
import org.osgi.framework.FrameworkUtil;

@SuppressWarnings("javadoc")
public final class ColumnarPreferenceUtils {

    private static final String COLUMNAR_SYMBOLIC_NAME =
        FrameworkUtil.getBundle(ColumnarTableBackend.class).getSymbolicName();

    static final ScopedPreferenceStore COLUMNAR_STORE =
        new ScopedPreferenceStore(InstanceScope.INSTANCE, COLUMNAR_SYMBOLIC_NAME);

    static final String CHUNK_SIZE_KEY = "knime.core.data.columnar.chunk-size";

    static final int CHUNK_SIZE_DEF = 28_000;

    static final String ENABLE_PHANTOM_REFERENCE_STORE_KEY = "knime.core.data.columnar.emnable-phantom-store";

    static final boolean ENABLE_PHANTOM_REFERENCE_STORE_DEF = true;

    static final String ENABLE_CACHED_STORE_KEY = "knime.core.data.columnar.emnable-cached-store";

    static final boolean ENABLE_CACHED_STORE_DEF = true;

    static final String ASYNC_FLUSH_NUM_THREADS_KEY = "knime.core.data.columnar.flush-num-threads";

    static final int ASYNC_FLUSH_NUM_THREADS_DEF = 4;

    private static final AtomicLong THREAD_COUNT = new AtomicLong();

    private static final ExecutorService ASYNC_FLUSH_EXECUTOR =
        Executors.newFixedThreadPool(COLUMNAR_STORE.getInt(ASYNC_FLUSH_NUM_THREADS_KEY),
            r -> new Thread(r, "KNIME-BackgroundColumnStoreWriter-" + THREAD_COUNT.incrementAndGet()));

    static final String COLUMN_DATA_CACHE_SIZE_KEY = "knime.core.data.columnar.data-cache-size";

    // the size (in MB) of the LRU cache for ColumnData of all tables
    static final int COLUMN_DATA_CACHE_SIZE_DEF = 1 << 10; // 1 GB

    private static final CachedColumnStoreCache COLUMN_DATA_CACHE =
        new CachedColumnStoreCache((long)COLUMNAR_STORE.getInt(COLUMN_DATA_CACHE_SIZE_KEY) << 20);

    static final String ENABLE_SMALL_STORE_KEY = "knime.core.data.columnar.emnable-small-store";

    static final boolean ENABLE_SMALL_STORE_DEF = true;

    static final String SMALL_TABLE_THRESHOLD_KEY = "knime.core.data.columnar.small-threshold";

    // the size (in MB) up to which a table is considered small
    static final int SMALL_TABLE_THRESHOLD_DEF = 1; // 1 MB

    static final String SMALL_TABLE_CACHE_SIZE_KEY = "knime.core.data.columnar.small-cache-size";

    // the size (in MB) of the LRU cache for entire small tables
    static final int SMALL_TABLE_CACHE_SIZE_DEF = 1 << 5; // 32 MB, i.e., holds up to 32 small tables

    private static final SmallColumnStoreCache SMALL_TABLE_CACHE =
        new SmallColumnStoreCache(COLUMNAR_STORE.getInt(SMALL_TABLE_THRESHOLD_KEY) << 20,
            (long)COLUMNAR_STORE.getInt(SMALL_TABLE_CACHE_SIZE_KEY) << 20);

    private ColumnarPreferenceUtils() {
    }

    public static int getChunkSize() {
        return COLUMNAR_STORE.getInt(CHUNK_SIZE_KEY);
    }

    @SuppressWarnings("resource")
    public static ColumnStore wrap(final ColumnStore store) {
        ColumnStore wrapped = store;
        if (COLUMNAR_STORE.getBoolean(ENABLE_CACHED_STORE_KEY)) {
            wrapped = new AsyncFlushCachedColumnStore(wrapped, COLUMN_DATA_CACHE, ASYNC_FLUSH_EXECUTOR);
        }
        if (COLUMNAR_STORE.getBoolean(ENABLE_SMALL_STORE_KEY)) {
            wrapped = new SmallColumnStore(wrapped, SMALL_TABLE_CACHE);
        }
        if (COLUMNAR_STORE.getBoolean(ENABLE_PHANTOM_REFERENCE_STORE_KEY)) {
            wrapped = PhantomReferenceStore.create(wrapped);
        }
        return wrapped;
    }

    @SuppressWarnings("resource")
    public static ColumnReadStore wrap(final ColumnReadStore store) {
        ColumnReadStore wrapped = store;
        if (COLUMNAR_STORE.getBoolean(ENABLE_CACHED_STORE_KEY)) {
            wrapped = new CachedColumnReadStore(wrapped, COLUMN_DATA_CACHE);
        }
        if (COLUMNAR_STORE.getBoolean(ENABLE_PHANTOM_REFERENCE_STORE_KEY)) {
            wrapped = PhantomReferenceReadStore.create(wrapped);
        }
        return wrapped;
    }

}
