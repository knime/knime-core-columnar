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

import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.OFF_HEAP_MEM_LIMIT_KEY;

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
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class ColumnarPreferenceUtils {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarPreferenceUtils.class);

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

    /** @return the max size of the heap (-Xmx) */
    static long getMaxHeapSize() {
        return Math.max(ManagementFactory.getMemoryPoolMXBeans().stream().filter(m -> m.getType() == MemoryType.HEAP)
            .map(MemoryPoolMXBean::getUsage).mapToLong(MemoryUsage::getMax).sum(), 0L);
    }

    static long getTotalPhysicalMemorySize() {
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

    private static int getNumThreads() {
        // Use the amount of processors divided by 2 but at least 1 and at most 8
        return Math.min(Math.max(Runtime.getRuntime().availableProcessors() / 2, 1), 8);
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

    /**
     * @return the cache for in-heap storing of object data
     */
    public static synchronized SharedObjectCache getHeapCache() {
        if (heapCache == null) {
            // NB: We always use the SoftReferencedObjectCache because it optimizes for performance compared to the
            // WeakReferencedObjectCache and we do not want to give the user too many options (see AP-20412)
            final var cache = new SoftReferencedObjectCache();
            MemoryAlertSystem.getInstanceUncollected().addListener(new MemoryAlertListener() {
                @Override
                protected boolean memoryAlert(final MemoryAlert alert) {
                    cache.invalidate();
                    return false;
                }
            });
            heapCache = cache;
        }
        return heapCache;
    }

    /**
     * @return the executor for serializing object data
     */
    public static synchronized ExecutorService getSerializeExecutor() {
        if (serializeExecutor == null) {
            serializeExecutor = ThreadUtils.executorServiceWithContext(Executors.newFixedThreadPool(getNumThreads(),
                r -> new Thread(r, "KNIME-ObjectSerializer-" + SERIALIZE_THREAD_COUNT.incrementAndGet())));
        }
        return serializeExecutor;
    }

    /**
     * @return the cache for storing entire small {@link BatchWritable BatchWritables} up to a given size
     */
    public static synchronized SharedBatchWritableCache getSmallTableCache() {
        if (smallTableCache == null) {
            // NB: The smallest number of tables that fit in the cache must be larger than the number of threads
            // to prevent a deadlock in the small table cache (AP-20535)
            var smallTableCacheSize = 32l << 20; // 32 MB
            var smallTableThreshold = 1 << 20; // 1 MB
            LOGGER.infoWithFormat("Small Table Cache size is %d MB", smallTableCacheSize >> 20);
            smallTableCache = new SharedBatchWritableCache(smallTableThreshold, smallTableCacheSize, getNumThreads());
        }
        return smallTableCache;
    }

    /**
     * @return the cache for storing general ReadData
     */
    public static synchronized SharedReadDataCache getColumnDataCache() {
        if (columnDataCache == null) {
            var columnDataCacheSize = (long)(getOffHeapMemoryLimit() * 0.8); // 80% of off-heap limit
            LOGGER.infoWithFormat("Column Data Cache size is %d MB.", columnDataCacheSize >> 20);
            columnDataCache = new SharedReadDataCache(columnDataCacheSize, getNumThreads());
        }
        return columnDataCache;
    }

    /**
     * @return the SharedReadBatchCache
     */
    public static synchronized SharedReadBatchCache getReadBatchCache() {
        if (batchCache == null) {
            var readBatchCacheSize = (long)(getOffHeapMemoryLimit() * 0.8); // 80% of off-heap limit
            LOGGER.infoWithFormat("Read Batch Cache size is %d MB.", readBatchCacheSize >> 20);
            batchCache = new SharedReadBatchCache(readBatchCacheSize);
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

    /**
     * @return the configured value for the maximum amount of off-heap memory that can be allocated (in MB)
     */
    public static int getOffHeapMemoryLimitMB() {
        return COLUMNAR_STORE.getInt(OFF_HEAP_MEM_LIMIT_KEY);
    }

    /**
     * @return the maximum amount of off-heap memory that can be allocated in bytes
     */
    public static long getOffHeapMemoryLimit() {
        return (long)getOffHeapMemoryLimitMB() << 20;
    }
}
