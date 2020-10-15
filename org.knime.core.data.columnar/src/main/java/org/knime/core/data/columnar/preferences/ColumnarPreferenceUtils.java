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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.ui.preferences.ScopedPreferenceStore;
import org.knime.core.columnar.cache.AsyncFlushCachedColumnStore;
import org.knime.core.columnar.cache.CachedColumnReadStore;
import org.knime.core.columnar.cache.CachedColumnStoreCache;
import org.knime.core.columnar.cache.SmallColumnStore;
import org.knime.core.columnar.cache.SmallColumnStore.SmallColumnStoreCache;
import org.knime.core.columnar.cache.heap.HeapCachedColumnReadStore;
import org.knime.core.columnar.cache.heap.HeapCachedColumnStore;
import org.knime.core.columnar.cache.heap.ObjectDataCache;
import org.knime.core.columnar.cache.heap.SoftReferencedObjectCache;
import org.knime.core.columnar.cache.heap.WeakReferencedObjectCache;
import org.knime.core.columnar.phantom.PhantomReferenceReadStore;
import org.knime.core.columnar.phantom.PhantomReferenceStore;
import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.data.columnar.ColumnarTableBackend;
import org.knime.core.data.util.memory.MemoryAlert;
import org.knime.core.data.util.memory.MemoryAlertListener;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.util.ThreadUtils;
import org.osgi.framework.FrameworkUtil;

@SuppressWarnings("javadoc")
public final class ColumnarPreferenceUtils {

    enum HeapCache {

            WEAK {
                @Override
                ObjectDataCache createCache() {
                    return new WeakReferencedObjectCache();
                }
            },

            SOFT {
                @Override
                ObjectDataCache createCache() {
                    final SoftReferencedObjectCache cache = new SoftReferencedObjectCache();

                    MemoryAlertSystem.getInstanceUncollected().addListener(new MemoryAlertListener() {
                        @Override
                        protected boolean memoryAlert(final MemoryAlert alert) {
                            cache.invalidate();
                            return false;
                        }
                    });

                    return cache;
                }
            };

        abstract ObjectDataCache createCache();
    }

    private static final String RESERVED_SIZE_PROPERTY = "knime.columnar.reservedmemorymb";

    // by default, reserve 4 GB for system
    private static final int RESERVED_SIZE = Integer.getInteger(RESERVED_SIZE_PROPERTY, 4096);

    private static final String COLUMNAR_SYMBOLIC_NAME =
        FrameworkUtil.getBundle(ColumnarTableBackend.class).getSymbolicName();

    static final ScopedPreferenceStore COLUMNAR_STORE =
        new ScopedPreferenceStore(InstanceScope.INSTANCE, COLUMNAR_SYMBOLIC_NAME);

    static final String DOMAIN_CALC_NUM_THREADS_KEY = "knime.core.data.columnar.domain-calc-num-threads";

    private static final AtomicLong DOMAIN_CALC_THREAD_COUNT = new AtomicLong();

    // lazily initialized
    private static ExecutorService DOMAIN_CALC_EXECUTOR;

    static final String HEAP_CACHE_NAME_KEY = "knime.core.data.columnar.heap-cache";

    // lazily initialized
    private static ObjectDataCache HEAP_CACHE;

    static final String SERIALIZE_NUM_THREADS_KEY = "knime.core.data.columnar.serialize-num-threads";

    private static final AtomicLong SERIALIZE_THREAD_COUNT = new AtomicLong();

    // lazily initialized
    private static ExecutorService SERIALIZE_EXECUTOR;

    // the size (in MB) of the LRU cache for entire small tables
    static final String SMALL_TABLE_CACHE_SIZE_KEY = "knime.core.data.columnar.small-cache-size";

    // the size (in MB) up to which a table is considered small
    static final String SMALL_TABLE_THRESHOLD_KEY = "knime.core.data.columnar.small-threshold";

    // lazily initialized
    private static SmallColumnStoreCache SMALL_TABLE_CACHE;

    // the size (in MB) of the LRU cache for ColumnData of all tables
    static final String COLUMN_DATA_CACHE_SIZE_KEY = "knime.core.data.columnar.data-cache-size";

    // lazily initialized
    private static CachedColumnStoreCache COLUMN_DATA_CACHE;

    static final String PERSIST_NUM_THREADS_KEY = "knime.core.data.columnar.flush-num-threads";

    private static final AtomicLong PERSIST_THREAD_COUNT = new AtomicLong();

    // lazily initialized
    private static ExecutorService PERSIST_EXECUTOR;

    private ColumnarPreferenceUtils() {
    }

    static long getMaxHeapSize() {
        return ManagementFactory.getMemoryPoolMXBeans().stream().filter(m -> m.getType() == MemoryType.HEAP)
            .map(MemoryPoolMXBean::getUsage).mapToLong(MemoryUsage::getMax).sum();
    }

    private static long getTotalPhysicalMemorySize() {
        try {
            // Unfortunately, there does not seem to be a safer way to determine the system's physical memory size.
            // The closest alternative would be ManagementFactory.getOperatingSystemMXBean::getTotalPhysicalMemorySize,
            // which is not supported in OpenJDK 8.
            return ((Long)ManagementFactory.getPlatformMBeanServer()
                .getAttribute(new ObjectName("java.lang", "type", "OperatingSystem"), "TotalPhysicalMemorySize"))
                    .longValue();
        } catch (ClassCastException | InstanceNotFoundException | AttributeNotFoundException
                | MalformedObjectNameException | ReflectionException | MBeanException ex) {
        }
        return 0L;
    }

    private static long getTotalFreeMemorySize() {
        try {
            return ((Long)ManagementFactory.getPlatformMBeanServer()
                .getAttribute(new ObjectName("java.lang", "type", "OperatingSystem"), "FreePhysicalMemorySize"))
                    .longValue();
        } catch (ClassCastException | InstanceNotFoundException | AttributeNotFoundException
                | MalformedObjectNameException | ReflectionException | MBeanException ex) {
        }
        return 0L;
    }

    /**
     * @return an estimate of the amount of available off-heap memory (in MB)
     */
    static int getUsablePhysicalMemorySizeMB() {
        final long jvmMemory = (long)(getMaxHeapSize() * 1.25); // add 25% to heap size for GC, code cache, etc.
        final long totalUnreservedMemory = getTotalPhysicalMemorySize() - ((long)RESERVED_SIZE << 20);
        final long freeMemorySans1G = getTotalFreeMemorySize() - (1L << 30); // reserve 1 GB for other applications

        final long usablePhysicalMemory = Math.max(Math.min(totalUnreservedMemory - jvmMemory, freeMemorySans1G), 0L);
        return (int)Math.min(usablePhysicalMemory >> 20, Integer.MAX_VALUE);
    }

    static int getNumAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    static int getDomainCalcNumThreads() {
        return COLUMNAR_STORE.getInt(DOMAIN_CALC_NUM_THREADS_KEY);
    }

    public static ExecutorService getDomainCalcExecutor() {
        if (DOMAIN_CALC_EXECUTOR == null) {
            DOMAIN_CALC_EXECUTOR =
                ThreadUtils.executorServiceWithContext(Executors.newFixedThreadPool(getDomainCalcNumThreads(),
                    r -> new Thread(r, "KNIME-DomainCalculator-" + DOMAIN_CALC_THREAD_COUNT.incrementAndGet())));
        }
        return DOMAIN_CALC_EXECUTOR;
    }

    static String getHeapCacheName() {
        return COLUMNAR_STORE.getString(HEAP_CACHE_NAME_KEY);
    }

    private static ObjectDataCache getHeapCache() {
        if (HEAP_CACHE == null) {
            HEAP_CACHE = HeapCache.valueOf(getHeapCacheName()).createCache();
        }
        return HEAP_CACHE;
    }

    static int getSerializeNumThreads() {
        return COLUMNAR_STORE.getInt(SERIALIZE_NUM_THREADS_KEY);
    }

    private static ExecutorService getSerializeExecutor() {
        if (SERIALIZE_EXECUTOR == null) {
            SERIALIZE_EXECUTOR =
                ThreadUtils.executorServiceWithContext(Executors.newFixedThreadPool(getDomainCalcNumThreads(),
                    r -> new Thread(r, "KNIME-ObjectSerializer-" + SERIALIZE_THREAD_COUNT.incrementAndGet())));
        }
        return SERIALIZE_EXECUTOR;
    }

    static int getSmallTableCacheSize() {
        return COLUMNAR_STORE.getInt(SMALL_TABLE_CACHE_SIZE_KEY);
    }

    private static int getSmallTableThreshold() {
        return COLUMNAR_STORE.getInt(SMALL_TABLE_THRESHOLD_KEY);
    }

    private static SmallColumnStoreCache getSmallTableCache() {
        if (SMALL_TABLE_CACHE == null) {
            final long smallTableCacheSize = (long)getSmallTableCacheSize() << 20;
            final long totalFreeMemorySize = getTotalFreeMemorySize();
            final int smallTableThreshold = (int)Math.min((long)getSmallTableThreshold() << 20, Integer.MAX_VALUE);

            if (smallTableCacheSize <= totalFreeMemorySize) {
                SMALL_TABLE_CACHE = new SmallColumnStoreCache(smallTableThreshold, smallTableCacheSize);
            } else {
                SMALL_TABLE_CACHE = new SmallColumnStoreCache(smallTableThreshold, 0);
                System.err.println(String.format(
                    "Small Table Cache is configured to be of size %dB, but only %dB of memory are available.",
                    smallTableCacheSize, totalFreeMemorySize));
            }
        }
        return SMALL_TABLE_CACHE;
    }

    static int getColumnDataCacheSize() {
        return COLUMNAR_STORE.getInt(COLUMN_DATA_CACHE_SIZE_KEY);
    }

    private static CachedColumnStoreCache getColumnDataCache() {
        if (COLUMN_DATA_CACHE == null) {
            final long columnDataCacheSize = (long)getColumnDataCacheSize() << 20;
            final long totalFreeMemorySize = getTotalFreeMemorySize();

            if (columnDataCacheSize <= totalFreeMemorySize) {
                COLUMN_DATA_CACHE = new CachedColumnStoreCache(columnDataCacheSize);
            } else {
                COLUMN_DATA_CACHE = new CachedColumnStoreCache(0);
                System.err.println(String.format(
                    "Column Data Cache is configured to be of size %dB, but only %dB of memory are available.",
                    columnDataCacheSize, totalFreeMemorySize));
            }
        }
        return COLUMN_DATA_CACHE;
    }

    static int getPersistNumThreads() {
        return COLUMNAR_STORE.getInt(PERSIST_NUM_THREADS_KEY);
    }

    private static ExecutorService getPersistExecutor() {
        if (PERSIST_EXECUTOR == null) {
            PERSIST_EXECUTOR =
                ThreadUtils.executorServiceWithContext(Executors.newFixedThreadPool(getPersistNumThreads(),
                    r -> new Thread(r, "KNIME-ColumnStoreWriter-" + PERSIST_THREAD_COUNT.incrementAndGet())));
        }
        return PERSIST_EXECUTOR;
    }

    @SuppressWarnings("resource")
    public static ColumnStore wrap(final ColumnStore store) {

        final CachedColumnStoreCache columnDataCache = getColumnDataCache();
        final SmallColumnStoreCache smallTableCache = getSmallTableCache();

        ColumnStore wrapped = store;

        if (columnDataCache.getMaxSizeInBytes() > 0) {
            wrapped = new AsyncFlushCachedColumnStore(wrapped, columnDataCache, getPersistExecutor());
        }
        if (smallTableCache.getMaxSize() > 0) {
            wrapped = new SmallColumnStore(wrapped, smallTableCache);
        }

        wrapped = new HeapCachedColumnStore(wrapped, getHeapCache(), getSerializeExecutor());
        return PhantomReferenceStore.create(wrapped);
    }

    @SuppressWarnings("resource")
    public static ColumnReadStore wrap(final ColumnReadStore store) {
        ColumnReadStore wrapped = store;
        if (getColumnDataCacheSize() > 0) {
            wrapped = new CachedColumnReadStore(wrapped, getColumnDataCache());
        }

        wrapped = new HeapCachedColumnReadStore(wrapped, getHeapCache());
        return PhantomReferenceReadStore.create(wrapped);
    }

}
