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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.knime.core.columnar.cache.AsyncFlushCachedColumnStore;
import org.knime.core.columnar.cache.CachedColumnReadStore;
import org.knime.core.columnar.cache.CachedColumnStoreCache;
import org.knime.core.columnar.cache.SmallColumnStore;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.phantom.PhantomReferenceReadStore;
import org.knime.core.columnar.phantom.PhantomReferenceStore;

public class ColumnStoreUtils {

    public static final String ERROR_MESSAGE_WRITER_CLOSED = "Column store writer has already been closed.";

    public static final String ERROR_MESSAGE_WRITER_NOT_CLOSED = "Column store writer has not been closed.";

    public static final String ERROR_MESSAGE_READER_CLOSED = "Column store reader has already been closed.";

    public static final String ERROR_MESSAGE_STORE_CLOSED = "Column store has already been closed.";

    // the size (in bytes) up to which a table is considered small
    private static final int SMALL_TABLE_THRESHOLD = 1 << 20; // 1 MB

    // the size (in bytes) of the LRU cache for entire small tables
    private static final long SMALL_TABLES_CACHE_SIZE = 1L << 25; // 32 MB, i.e., holds up to 32 small tables

    private static final SmallColumnStoreCache SMALL_TABLES_CACHE =
        new SmallColumnStoreCache(SMALL_TABLE_THRESHOLD, SMALL_TABLES_CACHE_SIZE);

    // the size (in bytes) of the LRU cache for ColumnData of all tables
    private static final long COLUMN_DATA_CACHE_SIZE = 1L << 30; // 1 GB

    private static final CachedColumnStoreCache COLUMN_DATA_CACHE = new CachedColumnStoreCache(COLUMN_DATA_CACHE_SIZE);

    private static final AtomicLong THREAD_COUNT = new AtomicLong();

    private static final ExecutorService ASYNC_FLUSH_EXECUTOR = Executors.newFixedThreadPool(4,
        r -> new Thread(r, "KNIME-BackgroundColumnStoreWriter-" + THREAD_COUNT.incrementAndGet()));

    private ColumnStoreUtils() {
    }

    public static ColumnStore cache(final ColumnStore store) {
        return PhantomReferenceStore.create(new SmallColumnStore(
            new AsyncFlushCachedColumnStore(store, COLUMN_DATA_CACHE, ASYNC_FLUSH_EXECUTOR), SMALL_TABLES_CACHE));
    }

    public static ColumnReadStore cache(final ColumnReadStore store) {
        return PhantomReferenceReadStore.create(new CachedColumnReadStore(store, COLUMN_DATA_CACHE));
    }

}
