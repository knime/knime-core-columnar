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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

import com.google.common.io.Files;

final class ArrowColumnStore implements ColumnStore {

    // TODO allow configurations of root allocator
    final static RootAllocator ROOT = new RootAllocator();

    final static String CFG_ARROW_CHUNK_SIZE = "CHUNK_SIZE";

    private final File m_file;

    private final BufferAllocator m_allocator;

    private final ColumnStoreSchema m_schema;

    private final ArrowColumnReadStore m_delegate;

    private final int m_chunkSize;

    private final ArrowColumnDataSpec<?>[] m_arrowSchema;

    ArrowColumnStore(final ColumnStoreSchema schema, final ArrowSchemaMapper mapper, final File file, final int chunkSize) {
        m_file = file;
        m_schema = schema;
        m_arrowSchema = mapper.map(schema);
        m_chunkSize = chunkSize;
        // TODO make configurable
        m_allocator = ROOT.newChildAllocator("ArrowColumnStore", 0, ROOT.getLimit());
        m_delegate = new ArrowColumnReadStore(schema, file);
    }

    @Override
    public void close() {
        m_delegate.close();
        m_file.delete();
        m_allocator.close();
    }

    @Override
    public ColumnDataWriter getWriter() {
        // TODO also write mapper type information for backwards-compatibility. Readers
        // can first get the mapper type from metadata and instantiate a mapper
        // themselves.
        return new ArrowColumnDataWriter(m_file, m_allocator, m_chunkSize);
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection config) {
        return m_delegate.createReader(config);
    }

    @Override
    public ColumnDataFactory getFactory() {
        return new ColumnDataFactory() {
            @Override
            public ColumnData[] create() {
                final ColumnData[] chunk = new ColumnData[m_arrowSchema.length];
                for (int i = 0; i < m_arrowSchema.length; i++) {
                    chunk[i] = m_arrowSchema[i].createEmpty(m_allocator);
                    chunk[i].ensureCapacity(m_chunkSize);
                }
                return chunk;
            }
        };
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_schema;
    }

    @Override
    public void saveToFile(final File f) throws IOException {
        Files.copy(m_file, f);
    }
}
