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
 *   24 Mar 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.cache.writable;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.cache.EvictingCache;
import org.knime.core.columnar.cache.SizeBoundLruCache;
import org.knime.core.columnar.memory.ColumnarOffHeapMemoryAlertSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A cache for storing entire {@link BatchWritable BatchWritables} up to a given size. The cache can be shared between
 * multiple {@link BatchWritableCache BatchWritableCaches}.
 */
public final class SharedBatchWritableCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(SharedBatchWritableCache.class);

    private final int m_sizeThreshold;

    private final EvictingCache<BatchWritableCache, ReadBatches> m_cache;

    private final long m_cacheSize;

    /**
     * @param sizeThreshold the size (in bytes) up to which to cache a writable
     * @param cacheSize the total size (in bytes) the cache should be able to hold
     * @param concurrencyLevel the allowed concurrency among update operations
     */
    public SharedBatchWritableCache(final int sizeThreshold, final long cacheSize, final int concurrencyLevel) {
        // NB: If the smallest number of tables that fit in the cache is smaller than the concurrencyLevel
        // this can cause a deadlock
        // TODO(AP-20535) make the cache robust against the deadlock and remove this precondition
        Preconditions.checkArgument(cacheSize / sizeThreshold > concurrencyLevel,
            "cacheSize / sizeThreshold must be larger than the cuncurrency level");

        m_sizeThreshold = sizeThreshold;
        m_cache = new SizeBoundLruCache<>(cacheSize, concurrencyLevel);
        m_cacheSize = cacheSize;

        ColumnarOffHeapMemoryAlertSystem.INSTANCE.addMemoryListener(() -> {
            if (m_cache.size() > 0) {
                LOGGER.debug("Received off-heap memory alert. Clearing cache.");
                m_cache.invalidateAll();
                return true;
            }
            LOGGER.debug("Received off-heap memory alert. Doing nothing because cache is empty.");
            return false;
        });
    }

    /**
     * @return total size (in bytes) the cache is able to hold
     */
    public final long getCacheSize() {
        return m_cacheSize;
    }

    int size() {
        return m_cache.size();
    }

    int getSizeThreshold() {
        return m_sizeThreshold;
    }

    EvictingCache<BatchWritableCache, ReadBatches> getCache() {
        return m_cache;
    }

}
