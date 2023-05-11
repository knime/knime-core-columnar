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
 *   Apr 6, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.batch.SharedReadBatchCache.BatchId;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.filter.ColumnSelection;

/**
 * Tests {@link SharedReadBatchCache}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class SharedReadBatchCacheTest {

    private SharedReadBatchCache m_cache;

    private ReadBatch m_batch;

    private ColumnSelection m_selection;

    private BatchId m_id;

    @BeforeEach
    void setup() {
        m_cache = new SharedReadBatchCache(1024);
        // TODO do via annotation
        m_batch = mock(ReadBatch.class);
        m_selection = mock(ColumnSelection.class);
        m_id = new BatchId(this, m_selection, 0);
    }

    @Test
    void testGetRetained() throws Exception {
        var optionalBatch = m_cache.getRetained(m_id);
        assertTrue(optionalBatch.isEmpty(), "No batch has been cached yet.");

        @SuppressWarnings("unchecked")
        Supplier<ReadBatch> mockSupplier = mock(Supplier.class);
        when(mockSupplier.get()).thenReturn(m_batch);

        var batch = m_cache.getRetained(m_id, mockSupplier);
        assertEquals(m_batch, batch, "Wrong batch returned.");
        verify(batch).retain();
        verify(mockSupplier).get();

        batch = m_cache.getRetained(m_id, mockSupplier);
        assertEquals(m_batch, batch, "Wrong batch returned.");
        verify(batch, times(2)).retain();
        // get shouldn't be called again because the batch is now in the cache
        verify(mockSupplier).get();

        optionalBatch = m_cache.getRetained(m_id);
        assertTrue(optionalBatch.isPresent(), "The batch is now in the cache.");
        batch = optionalBatch.get();
        assertEquals(m_batch, batch, "Wrong batch returned.");
        verify(batch, times(3)).retain();
    }

    @Test
    void testPut() throws Exception {
        assertTrue(m_cache.getRetained(m_id).isEmpty(), "The cache should be empty.");
        m_cache.put(m_id, m_batch);
        var batch = m_cache.getRetained(m_id);
        assertTrue(batch.isPresent(), "The batch should have been in the cache.");
        assertEquals(m_batch, batch.get());
    }

    @Test
    void testEvictAllFromSource() throws Exception {
        var idWithOtherSource = new BatchId(new Object(), m_selection, 0);
        var secondId = new BatchId(this, m_selection, 1);
        m_cache.put(m_id, m_batch);
        // in the real world it would be a different batch but for this scenario we don't care
        m_cache.put(secondId, m_batch);
        m_cache.put(idWithOtherSource, m_batch);
        assertTrue(m_cache.getRetained(m_id).isPresent(), "The key should have been in the cache.");
        assertTrue(m_cache.getRetained(secondId).isPresent(), "The key should have been in the cache.");
        assertTrue(m_cache.getRetained(idWithOtherSource).isPresent(), "The key should have been in the cache.");
        m_cache.evictAllFromSource(this);
        assertTrue(m_cache.getRetained(m_id).isEmpty(), "The key should have been evicted.");
        assertTrue(m_cache.getRetained(secondId).isEmpty(), "The key should have been evicted.");
        assertTrue(m_cache.getRetained(idWithOtherSource).isPresent(),
            "The key was from a different source and should therefore still be in the cache.");
    }

    @Test
    void testClear() throws Exception {
        var idWithOtherSource = new BatchId(new Object(), m_selection, 0);
        m_cache.put(m_id, m_batch);
        m_cache.put(idWithOtherSource, m_batch);
        assertTrue(m_cache.getRetained(m_id).isPresent(), "Key not in cache.");
        assertTrue(m_cache.getRetained(idWithOtherSource).isPresent(), "Key not in cache.");
        m_cache.clear();
        assertTrue(m_cache.getRetained(m_id).isEmpty(), "Cache should have been cleared.");
        assertTrue(m_cache.getRetained(idWithOtherSource).isEmpty(), "Cache should have been cleared.");
    }

    @Test
    void testConcurrentGetRetainedAndClear() throws Exception {
        var runRetains = new AtomicBoolean(true);
        var getRetainReturnedReleasedBatch = new AtomicBoolean(false);

        var getRetainedWithSupplierThread = new Thread(() -> {
            while (runRetains.get()) {
                var batch = m_cache.getRetained(m_id, TestBatch::new);
                if (isReleased(batch)) {
                    getRetainReturnedReleasedBatch.set(true);
                    return;
                }
            }
        });
        var getRetainedThread = new Thread(() -> {
            while (runRetains.get()) {
                var batch = m_cache.getRetained(m_id);
                if (batch.isPresent() && isReleased(batch.get())) {
                    getRetainReturnedReleasedBatch.set(true);
                    return;
                }
            }
        });

        getRetainedWithSupplierThread.start();
        getRetainedThread.start();

        for (int i = 0; i < 1_000_000; i++) {
            m_cache.clear();
        }
        runRetains.set(false);
        getRetainedThread.join();
        getRetainedWithSupplierThread.join();
        assertFalse(getRetainReturnedReleasedBatch.get(),
            "At least one getRetained call returned an already released batch.");
    }

    private static boolean isReleased(final ReadBatch batch) {
        return ((TestBatch)batch).isReleased();
    }

    private static final class TestBatch implements ReadBatch {

        private final AtomicInteger m_refCount = new AtomicInteger(1);

        @Override
        public int numData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NullableReadData[] getUnsafe() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void retain() {
            m_refCount.incrementAndGet();
        }

        @Override
        public void release() {
            m_refCount.decrementAndGet();
        }

        @Override
        public boolean tryRetain() {
            throw new UnsupportedOperationException();
        }

        boolean isReleased() {
            return m_refCount.get() <= 0;
        }

        @Override
        public long sizeOf() {
            return 1;
        }

        @Override
        public boolean isMissing(final int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NullableReadData get(final int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int length() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBatch decorate(final DataDecorator transformer) {
            throw new UnsupportedOperationException();
        }

    }

}
