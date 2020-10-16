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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.arrow.memory.BufferAllocator;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnDataReader;
import org.knime.core.columnar.store.ColumnDataWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreSchema;

/**
 * A {@link ColumnStore} implementation for Arrow.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class ArrowColumnStore implements ColumnStore {

    private final File m_file;

    private final ArrowColumnReadStore m_delegate;

    private final int m_chunkSize;

    private final ArrowColumnDataFactory[] m_factories;

    private final BufferAllocator m_allocator;

    ArrowColumnStore(final ColumnStoreSchema schema, final File file, final BufferAllocator allocator,
        final int chunkSize) {
        m_factories = ArrowSchemaMapper.map(schema);
        m_file = file;
        m_allocator = allocator;
        m_chunkSize = chunkSize;
        m_delegate = new ArrowColumnReadStore(schema, file, allocator);
    }

    @Override
    public ColumnDataWriter getWriter() {
        return new ArrowColumnDataWriter(m_file, m_chunkSize, m_factories);
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection config) {
        return m_delegate.createReader(config);
    }

    @Override
    public ColumnDataFactory getFactory() {
        return () -> {
            final ColumnWriteData[] chunk = new ColumnWriteData[m_factories.length];
            for (int i = 0; i < m_factories.length; i++) {
                chunk[i] =
                    ArrowColumnDataFactory.createWrite(m_factories[i], String.valueOf(i), m_allocator, m_chunkSize);
            }
            return new DefaultWriteBatch(chunk, m_chunkSize);
        };
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_delegate.getSchema();
    }

    @Override
    public void save(final File f) throws IOException {
        Files.copy(m_file.toPath(), f.toPath());
    }

    @Override
    public void close() throws IOException {
        m_delegate.close();
        if (m_file.exists()) {
            Files.delete(m_file.toPath());
        }
        m_allocator.close();
    }
}
