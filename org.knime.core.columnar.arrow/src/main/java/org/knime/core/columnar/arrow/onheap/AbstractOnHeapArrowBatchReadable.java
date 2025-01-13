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
 *   Jul 14, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow.onheap;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;
import org.knime.core.columnar.arrow.ArrowIpcFileStore;
import org.knime.core.columnar.batch.BatchReadable;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Abstract implementation of a {@link BatchReadable} for Arrow IPC files. Holds the {@link ColumnarSchema schema},
 * {@link Path path} and {@link BufferAllocator allocator}. On #close() the allocator is closed but an
 * {@link IOException} is thrown if there is still memory allocated by the allocator.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractOnHeapArrowBatchReadable implements BatchReadable, ArrowIpcFileStore {

    protected final ColumnarSchema m_schema;

    protected final FileHandle m_fileHandle;

    protected final BufferAllocator m_allocator;

    AbstractOnHeapArrowBatchReadable(final ColumnarSchema schema, final FileHandle file, final BufferAllocator allocator) {
        m_schema = schema;
        m_fileHandle = file;
        m_allocator = allocator;
    }

    /**
     * @return the fileHandle
     */
    @Override
    public FileHandle getFileHandle() {
        return m_fileHandle;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    protected BufferAllocator getAllocator() {
        return m_allocator;
    }

    @Override
    public void close() throws IOException {
        final long allocated = m_allocator.getAllocatedMemory();
        m_allocator.close();
        if (allocated > 0) {
            throw new IOException(
                String.format("Store closed with unreleased data. %d bytes of memory leaked.", allocated));
        }
    }
}
