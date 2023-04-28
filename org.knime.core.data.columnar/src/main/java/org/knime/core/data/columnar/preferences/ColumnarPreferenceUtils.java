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

import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.COLUMN_DATA_CACHE_SIZE_DEF;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.COLUMN_DATA_CACHE_SIZE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.HEAP_CACHE_NAME_DEF;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.HEAP_CACHE_NAME_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.NUM_THREADS_DEF;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.NUM_THREADS_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.SMALL_TABLE_CACHE_SIZE_DEF;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.SMALL_TABLE_CACHE_SIZE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.SMALL_TABLE_THRESHOLD_DEF;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.SMALL_TABLE_THRESHOLD_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.USE_DEFAULTS_KEY;

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
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.cache.batch.SharedReadBatchCache;
import org.knime.core.columnar.cache.data.SharedReadDataCache;
import org.knime.core.columnar.cache.object.shared.SharedObjectCache;
import org.knime.core.columnar.cache.object.shared.SoftReferencedObjectCache;
import org.knime.core.columnar.cache.object.shared.WeakReferencedObjectCache;
import org.knime.core.columnar.cache.writable.SharedBatchWritableCache;
import org.knime.core.data.columnar.ColumnarTableBackend;
import org.knime.core.data.util.memory.MemoryAlert;
import org.knime.core.data.util.memory.MemoryAlertListener;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.ThreadUtils;
import org.osgi.framework.FrameworkUtil;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class ColumnarPreferenceUtils {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarPreferenceUtils.class);

    enum HeapCache {

            WEAK {
                @Override
                SharedObjectCache createCache() {
                    return new WeakReferencedObjectCache();
                }
            },

            SOFT {
                @Override
                SharedObjectCache createCache() {
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

        abstract SharedObjectCache createCache();
    }

    private static final String RESERVED_SIZE_PROPERTY = "knime.columnar.reservedmemorymb";

    // by default, reserve 4 GB for system
    private static final int RESERVED_SIZE = Integer.getInteger(RESERVED_SIZE_PROPERTY, 4096);

    private static final String COLUMNAR_SYMBOLIC_NAME =
        FrameworkUtil.getBundle(ColumnarTableBackend.class).getSymbolicName();

    static final ScopedPreferenceStore COLUMNAR_STORE =
        new ScopedPreferenceStore(InstanceScope.INSTANCE, COLUMNAR_SYMBOLIC_NAME);

    private static final AtomicLong DUPLICATE_CHECK_THREAD_COUNT = new AtomicLong();

    // lazily initialized
    private static ExecutorService duplicateCheckExecutor;

    private static final AtomicLong DOMAIN_CALC_THREAD_COUNT = new AtomicLong();

    // lazily initialized
    private static ExecutorService domainCalcExecutor;

    // lazily initialized
    private static SharedObjectCache heapCache;

    private static final AtomicLong SERIALIZE_THREAD_COUNT = new AtomicLong();

    // lazily initialized
    private static ExecutorService serializeExecutor;

    // lazily initialized
    private static SharedBatchWritableCache smallTableCache;

    // lazily initialized
    private static SharedReadDataCache columnDataCache;

    private static SharedReadBatchCache batchCache;

    private static final AtomicLong PERSIST_THREAD_COUNT = new AtomicLong();

    // lazily initialized
    private static ExecutorService persistExecutor;

    private ColumnarPreferenceUtils() {
    }

    static long getMaxHeapSize() {
        return Math.max(ManagementFactory.getMemoryPoolMXBeans().stream().filter(m -> m.getType() == MemoryType.HEAP)
            .map(MemoryPoolMXBean::getUsage).mapToLong(MemoryUsage::getMax).sum(), 0L);
    }

    private static long getTotalPhysicalMemorySize() {
        try {
            // Unfortunately, there does not seem to be a safer way to determine the system's physical memory size.
            // The closest alternative would be ManagementFactory.getOperatingSystemMXBean::getTotalPhysicalMemorySize,
            // which is not supported in OpenJDK 8.
            return Math
                .max(
                    ((Long)ManagementFactory.getPlatformMBeanServer().getAttribute(
                        new ObjectName("java.lang", "type", "OperatingSystem"), "TotalPhysicalMemorySize")).longValue(),
                    0L);
        } catch (ClassCastException | InstanceNotFoundException | AttributeNotFoundException
                | MalformedObjectNameException | ReflectionException | MBeanException ex) {
            LOGGER.warn("Error while attempting to determine total physical memory size", ex);
        }
        return 0L;
    }

    /**
     * @return the amount of free memory in bytes.
     *
     * Note: On Mac, this returns unuseable information because MacOS caches everything in RAM,
     * even closed application etc. and calls this "cached" and not "free". So if a Mac has been running
     * for some time, the amount of free memory will be pretty much zero.
     */
    private static long getTotalFreeMemorySize() {
        try {
            return Math.max(((Long)ManagementFactory.getPlatformMBeanServer()
                .getAttribute(new ObjectName("java.lang", "type", "OperatingSystem"), "FreePhysicalMemorySize"))
                    .longValue(),
                0L);
        } catch (ClassCastException | InstanceNotFoundException | AttributeNotFoundException
                | MalformedObjectNameException | ReflectionException | MBeanException ex) {
            LOGGER.warn("Error while attempting to determine total free memory size", ex);
        }
        return 0L;
    }

    /**
     * @return an estimate of the amount of available off-heap memory (in MB)
     */
    static int getUsablePhysicalMemorySizeMB() {
        final long jvmMemory = (long)(getMaxHeapSize() * 1.25); // add 25% to heap size for GC, code cache, etc.
        final long totalUnreservedMemory = getTotalPhysicalMemorySize() - ((long)RESERVED_SIZE << 20);

        // Because getTotalFreeMemorySize() is not reliable on Mac, we ignore the free memory
        // size and return half of the remaining physical memory.
        if (operatingSystemIsMac()) {
            return (int)Math.min(Math.max(0, totalUnreservedMemory - jvmMemory) >> 20, Integer.MAX_VALUE) / 2;
        }

        final long freeMemorySans1G = getTotalFreeMemorySize() - (1L << 30); // reserve 1 GB for other applications

        final long usablePhysicalMemory = Math.max(Math.min(totalUnreservedMemory - jvmMemory, freeMemorySans1G), 0L);
        return (int)Math.min(usablePhysicalMemory >> 20, Integer.MAX_VALUE);
    }

    static int getNumAvailableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    static boolean useDefaults() {
        return COLUMNAR_STORE.getBoolean(USE_DEFAULTS_KEY);
    }

    static int getNumThreads() {
        return useDefaults() ? NUM_THREADS_DEF : COLUMNAR_STORE.getInt(NUM_THREADS_KEY);
    }

    /**
     * @return the executor for performing duplicate row id checks
     */
    public static synchronized ExecutorService getDuplicateCheckExecutor() {
        if (duplicateCheckExecutor == null) {
            duplicateCheckExecutor =
                ThreadUtils.executorServiceWithContext(Executors.newFixedThreadPool(getNumThreads(),
                    r -> new Thread(r, "KNIME-DuplicateChecker-" + DUPLICATE_CHECK_THREAD_COUNT.incrementAndGet())));
        }
        return duplicateCheckExecutor;
    }

    /**
     * @return the executor for calculating domains
     */
    public static synchronized ExecutorService getDomainCalcExecutor() {
        if (domainCalcExecutor == null) {
            domainCalcExecutor = ThreadUtils.executorServiceWithContext(Executors.newFixedThreadPool(getNumThreads(),
                r -> new Thread(r, "KNIME-DomainCalculator-" + DOMAIN_CALC_THREAD_COUNT.incrementAndGet())));
        }
        return domainCalcExecutor;
    }

    static String getHeapCacheName() {
        return useDefaults() ? HEAP_CACHE_NAME_DEF : COLUMNAR_STORE.getString(HEAP_CACHE_NAME_KEY);
    }

    /**
     * @return the cache for in-heap storing of object data
     */
    public static synchronized SharedObjectCache getHeapCache() {
        if (heapCache == null) {
            heapCache = HeapCache.valueOf(getHeapCacheName()).createCache();
        }
        return heapCache;
    }

    /**
     * @return the executor for serializing object data
     */
    public static ExecutorService getSerializeExecutor() {
        if (serializeExecutor == null) {
            serializeExecutor = ThreadUtils.executorServiceWithContext(Executors.newFixedThreadPool(getNumThreads(),
                r -> new Thread(r, "KNIME-ObjectSerializer-" + SERIALIZE_THREAD_COUNT.incrementAndGet())));
        }
        return serializeExecutor;
    }

    static int getSmallTableCacheSize() {
        return useDefaults() ? SMALL_TABLE_CACHE_SIZE_DEF : COLUMNAR_STORE.getInt(SMALL_TABLE_CACHE_SIZE_KEY);
    }

    private static int getSmallTableThreshold() {
        return useDefaults() ? SMALL_TABLE_THRESHOLD_DEF : COLUMNAR_STORE.getInt(SMALL_TABLE_THRESHOLD_KEY);
    }

    /**
     * @return the cache for storing entire small {@link BatchWritable BatchWritables} up to a given size
     */
    public static synchronized SharedBatchWritableCache getSmallTableCache() {
        if (smallTableCache == null) {
            long smallTableCacheSize = (long)getSmallTableCacheSize() << 20;
            long totalFreeMemorySize = getTotalFreeMemorySize();

            if (operatingSystemIsMac()) {
                // Because getTotalFreeMemorySize() is not reliable on Mac, we ignore the free memory
                // size and rely on the usable physical memory size and hope that the OS clears the
                // unused but cached things from the RAM if needed.
                totalFreeMemorySize = ((long)getUsablePhysicalMemorySizeMB()) << 20;
            }

            final int smallTableThreshold = (int)Math.min((long)getSmallTableThreshold() << 20, Integer.MAX_VALUE);

            if (smallTableCacheSize <= 0) {
                smallTableCache = new SharedBatchWritableCache(smallTableThreshold, 0, getNumThreads());
                LOGGER.warn(
                    "Small Table Cache is disabled. Consider enabling it in the Columnar Table Backend preferences");
            } else if (smallTableCacheSize <= totalFreeMemorySize) {
                LOGGER.infoWithFormat("Small Table Cache size is %d MB", smallTableCacheSize >> 20);
                smallTableCache =
                    new SharedBatchWritableCache(smallTableThreshold, smallTableCacheSize, getNumThreads());
            } else {
                smallTableCache =
                    new SharedBatchWritableCache(smallTableThreshold, totalFreeMemorySize, getNumThreads());
                LOGGER.warnWithFormat(
                    "Small Table Cache is configured to be of size %d MB, but only %d MB of memory are available.",
                    smallTableCacheSize >> 20, totalFreeMemorySize >> 20);
            }
        }
        return smallTableCache;
    }

    /**
     * @return the size of the cache for storing general ReadData
     */
    public static int getColumnDataCacheSize() {
        return useDefaults() ? COLUMN_DATA_CACHE_SIZE_DEF : COLUMNAR_STORE.getInt(COLUMN_DATA_CACHE_SIZE_KEY);
    }

    private static boolean operatingSystemIsMac() {
        String os = System.getProperty("os.name").toLowerCase();
        return os.contains("mac");
    }

    /**
     * @return the cache for storing general ReadData
     */
    public static synchronized SharedReadDataCache getColumnDataCache() {
        if (columnDataCache == null) {
            final long columnDataCacheSize = (long)getColumnDataCacheSize() << 20;
            final long smallTableCacheSize = getSmallTableCache().getCacheSize();
            long totalFreeMemorySize = Math.max(getTotalFreeMemorySize() - smallTableCacheSize, 0L);

            if (operatingSystemIsMac()) {
                // Because getTotalFreeMemorySize() is not reliable on Mac, we ignore the free memory
                // size and rely on the usable physical memory size and hope that the OS clears the
                // unused but cached things from the RAM if needed.
                totalFreeMemorySize = (((long)getUsablePhysicalMemorySizeMB()) << 20) - smallTableCacheSize;
            }

            if (columnDataCacheSize <= 0) {
                columnDataCache = new SharedReadDataCache(0, getNumThreads());
                LOGGER.warn(
                    "Column Data Cache is disabled. Consider enabling it in the Columnar Table Backend preferences");
            } else if (columnDataCacheSize <= totalFreeMemorySize) {
                LOGGER.infoWithFormat("Column Data Cache size is %d MB.", columnDataCacheSize >> 20);
                columnDataCache = new SharedReadDataCache(columnDataCacheSize, getNumThreads());
            } else {
                columnDataCache = new SharedReadDataCache(totalFreeMemorySize, getNumThreads());
                LOGGER.warnWithFormat(
                    "Column Data Cache is configured to be of size %d MB, but only %d MB of memory are available.",
                    columnDataCacheSize >> 20, totalFreeMemorySize >> 20);
            }
            // TODO AP-20371: Clear data cache if the overall off-heap size becomes critical
        }
        return columnDataCache;
    }

    private static long getOffheapSizeInBytes() {
        return (long)getColumnDataCacheSize() << 20;
    }

    /**
     * @return the SharedReadBatchCache
     */
    public static synchronized SharedReadBatchCache getReadBatchCache() {
        if (batchCache == null) {
            batchCache = new SharedReadBatchCache((long)(getOffheapSizeInBytes() * 0.8));
            // TODO AP-20371: Clear batch cache if the overall off-heap size becomes critical
        }
        return batchCache;
    }

    /**
     * @return the executor for persisting data from memory to disk
     */
    public static synchronized ExecutorService getPersistExecutor() {
        if (persistExecutor == null) {
            persistExecutor = ThreadUtils.executorServiceWithContext(Executors.newFixedThreadPool(getNumThreads(),
                r -> new Thread(r, "KNIME-ColumnStoreWriter-" + PERSIST_THREAD_COUNT.incrementAndGet())));
        }
        return persistExecutor;
    }

}
