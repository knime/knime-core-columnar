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

import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link ColumnStoreFactory} implementation for Arrow.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class ArrowColumnStoreFactory implements ColumnStoreFactory {

    private static final RootAllocator ROOT = new RootAllocator();

    private final BufferAllocator m_allocator;

    private long m_initReservation;

    private long m_maxAllocation;

    private final ArrowCompression m_compression;

    /**
     * Create a {@link ColumnStoreFactory} for Arrow using the default root allocator.
     */
    public ArrowColumnStoreFactory() {
        this(ROOT, 0, ROOT.getLimit());
    }

    /**
     * Create a {@link ColumnStoreFactory} for Arrow using the given allocator. For each {@link BatchStore} and
     * {@link BatchReadStore} the allocator is used to create a child allocator with the given initial reservation and
     * max allocation.
     *
     * @param allocator the allocator to use
     * @param initReservation the initial reservation for a child allocator
     * @param maxAllocation the maximum alloaction for the child allocator
     */
    public ArrowColumnStoreFactory(final BufferAllocator allocator, final long initReservation,
        final long maxAllocation) {
        m_allocator = allocator;
        m_initReservation = initReservation;
        m_maxAllocation = maxAllocation;
        m_compression = ArrowCompressionUtil.getDefaultCompression();
    }

    @Override
    @SuppressWarnings("resource") // Allocator closed by store
    public ArrowBatchStore createStore(final ColumnarSchema schema, final Path path) {
        final BufferAllocator allocator =
            m_allocator.newChildAllocator("ArrowColumnStore", m_initReservation, m_maxAllocation);
        return new ArrowBatchStore(schema, path, m_compression, allocator);
    }

    @Override
    @SuppressWarnings("resource") // Allocator closed by store
    public ArrowBatchReadStore createReadStore(final ColumnarSchema schema, final Path path) {
        final BufferAllocator allocator =
            m_allocator.newChildAllocator("ArrowColumnReadStore", m_initReservation, m_maxAllocation);
        return new ArrowBatchReadStore(schema, path, allocator);
    }
}
