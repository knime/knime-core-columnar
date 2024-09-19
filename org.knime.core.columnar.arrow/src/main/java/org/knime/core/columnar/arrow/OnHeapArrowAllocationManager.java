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
 *   Sep 18, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * The good allocation manager!
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class OnHeapArrowAllocationManager extends AllocationManager {

    //    private static final Unsafe UNSAFE = jdk.internal.misc.Unsafe.getUnsafe();

    // TODO is the memory address used? It's empty
    private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP, null, 0, 0);

    public static final AllocationManager.Factory FACTORY = new Factory() {
        @Override
        public AllocationManager create(final BufferAllocator accountingAllocator, final long size) {
            return new OnHeapArrowAllocationManager(accountingAllocator, size);
        }

        @Override
        public ArrowBuf empty() {
            return EMPTY;
        }
    };

    private byte[] m_memory;

    protected OnHeapArrowAllocationManager(final BufferAllocator accountingAllocator, final long size) {
        super(accountingAllocator);

        if (size > Integer.MAX_VALUE) {
            // TODO support larger sizes
            throw new IllegalArgumentException("Size too large: " + size);
        }

        m_memory = new byte[(int)size];
    }

    @Override
    public long getSize() {
        return m_memory.length;
    }

    @Override
    protected long memoryAddress() {
        return MemoryUtil.UNSAFE.arrayBaseOffset(byte[].class) + MemoryUtil.UNSAFE.getLong(m_memory, 0L);
    }

    @Override
    protected void release0() {
        m_memory = null;
    }
}
