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
 *   Apr 28, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.cache.batch.SharedReadBatchCache.BatchId;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.DefaultColumnSelection;

/**
 * Abstract test suite for batch caches that covers common methods.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractBatchCacheTest {

    protected RandomAccessBatchReader m_reader;

    protected ReadBatch m_batch;

    protected RandomAccessBatchReadable m_readable;

    protected SharedReadBatchCache m_sharedCache;

    protected ColumnSelection m_selection;

    @BeforeEach
    protected void setup() {
        m_reader = mock(RandomAccessBatchReader.class);
        m_batch = mock(ReadBatch.class);
        m_readable = mock(RandomAccessBatchReadable.class);
        m_sharedCache = new SharedReadBatchCache(8 << 10); // 8KiB
        m_selection = new DefaultColumnSelection(4);
    }

    protected abstract RandomAccessBatchReadable getCache();

    protected void stubReadRetained() throws IOException {
        when(m_reader.readRetained(anyInt())).thenReturn(m_batch);
    }

    @SuppressWarnings("resource")
    protected void stubCreateReader() {
        when(m_readable.createRandomAccessReader(any())).thenReturn(m_reader);
    }

    @Test
    void testClosingReaderClosesDelegateReader() throws Exception {
        stubCreateReader();
        try (@SuppressWarnings("resource")
        var reader = getCache().createRandomAccessReader(m_selection)) {
            verify(m_reader, never()).close();
        }
        verify(m_reader).close();
    }

    @Test
    void testClosingCacheClosesDelegate() throws Exception {
        verify(m_readable, never()).close();
        getCache().close();
        verify(m_readable).close();
    }

    @SuppressWarnings("resource")
    @Test
    void testCloseEvictsBatches() throws Exception {
        stubCreateReader();
        stubReadRetained();
        try (var cache = getCache()) {
            @SuppressWarnings("resource")
            var cachedReader = cache.createRandomAccessReader(m_selection);
            var readBatch = cachedReader.readRetained(0);
            assertEquals(m_batch, readBatch, "Unexpected batch.");
        }

        assertTrue(m_sharedCache.getRetained(new BatchId(getCache(), m_selection, 0)).isEmpty(),
            "Close should have evicted all batches from the shared cache that originated from the readable.");
    }


}
