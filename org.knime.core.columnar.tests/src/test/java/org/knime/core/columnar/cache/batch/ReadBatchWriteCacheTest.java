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
 *   Apr 12, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.filter.FilteredColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DefaultDataTraits;

/**
 * Tests {@link ReadBatchWriteCache}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ReadBatchWriteCacheTest extends AbstractBatchCacheTest {

    private static final ColumnarSchema SCHEMA = new DefaultColumnarSchema(//
        Stream.generate(DataSpec::stringSpec).limit(3).toList(), //
        Stream.generate(DefaultDataTraits::empty).limit(3).toList()//
    );

    private BatchWritable m_writable;

    private BatchWriter m_writer;

    private ReadBatchWriteCache m_cache;

    @Override
    @SuppressWarnings("resource")
    @BeforeEach
    protected void setup() {
        super.setup();
        m_writer = mock(BatchWriter.class);
        m_writable = mock(BatchWritable.class);
        when(m_writable.getSchema()).thenReturn(SCHEMA);
        when(m_writable.getWriter()).thenReturn(m_writer);
        m_cache = new ReadBatchWriteCache(m_writable, m_readable, m_sharedCache);
    }

    @Override
    protected RandomAccessBatchReadable getCache() {
        return m_cache;
    }

    @Override
    void testCloseEvictsBatches() throws Exception {
        writeBatch();
        super.testCloseEvictsBatches();
    }

    @Override
    void testClosingCacheClosesDelegate() throws Exception {
        verify(m_writer, never()).close();
        super.testClosingCacheClosesDelegate();
        verify(m_writer).close();
    }

    @SuppressWarnings("resource")
    @Test
    void testWriteThenRead() throws Exception {
        when(m_readable.createRandomAccessReader(any())).thenReturn(m_reader);
        writeBatch();
        try (var reader = m_cache.createRandomAccessReader()) {
            when(m_reader.readRetained(0)).thenReturn(m_batch);
            var batch = reader.readRetained(0);
            assertEquals(m_batch, batch, "Expected the batch that was just written.");
            verify(m_reader, never()).readRetained(0);
            m_sharedCache.clear();
            batch = reader.readRetained(0);
            assertEquals(m_batch, batch, "Expected the batch to be read from the underlying reader.");
            verify(m_reader).readRetained(0);
        }
    }

    private void writeBatch() throws IOException {
        try (var writer = m_cache.getWriter()) {
            writer.write(m_batch);
        }
    }

    /**
     * If a cache miss occurs for a filtered ColumnSelection, the reader should attempt to first get the full batch
     * which might still be in the cache from writing before reading the batch from the delegate.
     */
    @SuppressWarnings("resource")
    @Test
    void testCacheMissOnSelectionFallsBackToFullBatch() throws Exception {
        writeBatch();
        stubCreateReader();
        var selection = new FilteredColumnSelection(3, 1);
        try (var reader = m_cache.createRandomAccessReader(selection)) {
            var batch = reader.readRetained(0);
            assertEquals(m_batch, batch, "Expected the batch that was just written.");
            verify(m_reader, never()).readRetained(0);
        }
    }

}
