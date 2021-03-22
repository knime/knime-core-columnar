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
package org.knime.core.columnar.arrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.BatchWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 * A {@link ColumnStore} implementation for Arrow.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class ArrowColumnStore implements ColumnStore {

    private final Path m_path;

    private final ArrowColumnReadStore m_delegate;

    private final ArrowColumnDataFactory[] m_factories;

    private final BufferAllocator m_allocator;

    private final ArrowCompression m_compression;

    private final ArrowColumnDataWriter m_writer;

    ArrowColumnStore(final ColumnStoreSchema schema, final Path path, final ArrowCompression compression,
        final BufferAllocator allocator) {
        m_factories = ArrowSchemaMapper.map(schema);
        m_path = path;
        m_compression = compression;
        m_allocator = allocator;
        m_writer = new ArrowColumnDataWriter(path.toFile(), m_factories, m_compression, m_allocator);
        m_delegate = new ArrowColumnReadStore(schema, path, allocator, m_writer.m_numBatches, m_writer.m_chunkSize);
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public BatchReader createReader(final ColumnSelection config) {
        return m_delegate.createReader(config);
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public int numBatches() {
        return m_delegate.numBatches();
    }

    @Override
    public int maxLength() {
        return m_delegate.maxLength();
    }

    @Override
    public void flush() {
        // nothing to do; this store is always flushed
    }
    
    @Override
    public void close() throws IOException {
        // m_allocator is closed via the delegate.
        m_delegate.close();
        Files.deleteIfExists(m_path);
    }

}
