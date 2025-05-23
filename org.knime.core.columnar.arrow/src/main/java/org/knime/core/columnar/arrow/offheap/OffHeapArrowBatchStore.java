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
package org.knime.core.columnar.arrow.offheap;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.knime.core.columnar.arrow.ArrowBatchStore;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.OffsetProvider;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link BatchStore} implementation for Arrow.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OffHeapArrowBatchStore extends AbstractOffHeapArrowBatchReadable implements ArrowBatchStore {

    private final OffHeapArrowColumnDataFactory[] m_factories;

    private final OffHeapArrowBatchWriter m_writer;

    /**
     * Creates a new {@link OffHeapArrowBatchStore}.
     *
     * @param schema
     * @param fileSupplier
     * @param compression
     * @param allocator
     */
    public OffHeapArrowBatchStore(final ColumnarSchema schema, final FileHandle fileSupplier,
        final ArrowCompression compression, final BufferAllocator allocator) {
        super(schema, fileSupplier, allocator);
        m_factories = OffHeapArrowSchemaMapper.map(schema);
        m_writer = new OffHeapArrowBatchWriter(fileSupplier, m_factories, compression, m_allocator);
    }

    @Override
    public BatchWriter getWriter() {

        return m_writer;
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection config) {
        return new OffHeapArrowBatchReader(getFileHandle().asFile(), m_allocator, m_factories, config);
    }

    @Override
    public int numBatches() {
        return m_writer.numBatches();
    }

    @Override
    public long[] getBatchBoundaries() {
        return m_writer.getBatchBoundaries();
    }

    @Override
    public long numRows() {
        return m_writer.numRows();
    }

    /**
     * @return an object that can provide the offsets of record batches and dictionary batches in an Arrow IPC file. If
     *         new batches are written to the file after this method was called, the object will also provide offsets to
     *         the newly written batches.
     */
    @Override
    public OffsetProvider getOffsetProvider() {
        return m_writer.getOffsetProvider();
    }

    @Override
    public void close() throws IOException {
        m_writer.close();
        super.close();
    }

}
