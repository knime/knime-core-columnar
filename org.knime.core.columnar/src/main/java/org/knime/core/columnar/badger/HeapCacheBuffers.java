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
 *   22 Dec 2023 (chaubold): created
 */
package org.knime.core.columnar.badger;

import java.util.Arrays;
import java.util.stream.Stream;

import org.knime.core.columnar.cache.DataIndex;
import org.knime.core.columnar.cache.object.CachedDataFactoryBuilder;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.VarBinaryAccess.VarBinaryReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;
import org.knime.core.table.schema.traits.StructDataTraits;

/**
 * Built very similar to {@link CachedDataFactoryBuilder}
 *
 * @author chaubold
 */
public final class HeapCacheBuffers {
    private HeapCacheBuffers() {
    }

    /**
     * @param schema
     * @return A {@link HeapCacheBuffer} for each column in the table schema.
     */
    public static HeapCacheBuffer<?>[] createHeapCacheBuffers(final ColumnarSchema schema) {
        final var buffers = new HeapCacheBuffer[schema.numColumns()];
        Arrays.setAll(buffers,
            i -> createHeapCacheBuffer(schema.getSpec(i), schema.getTraits(i), DataIndex.createColumnIndex(i)));
        return buffers;
    }

    private static HeapCacheBuffer<?> createHeapCacheBuffer(final DataSpec spec, final DataTraits traits,
        final DataIndex index) {
        if (spec instanceof StringDataSpec) {
            return new StringHeapCacheBuffer();
        } else if (spec instanceof VarBinaryDataSpec) {
            return new VarBinaryHeapCacheBuffer();
        } else if (spec instanceof StructDataSpec) {
            var structSpec = (StructDataSpec)spec;
            var structTraits = (StructDataTraits)traits;
            return createHeapCacheStructBuffer(index, structSpec, structTraits);
        } else if (spec instanceof ListDataSpec) {
            var listSpec = (ListDataSpec)spec;
            var listTraits = (ListDataTraits)traits;
            return createHeapCacheListBuffer(index, listSpec, listTraits);
        } else {
            return UncachedHeapBuffer.INSTANCE;
        }
    }

    private static HeapCacheBuffer<?> createHeapCacheListBuffer(final DataIndex index, final ListDataSpec listSpec,
        final ListDataTraits listTraits) {
        return UncachedHeapBuffer.INSTANCE;
        // TODO AP-18333: Properly implement caching lists of objects
    }

    /**
     * @param index
     * @param structSpec
     * @param structTraits
     * @return
     */
    private static HeapCacheBuffer<?> createHeapCacheStructBuffer(final DataIndex index,
        final StructDataSpec structSpec, final StructDataTraits structTraits) {
        var innerBuffers = new HeapCacheBuffer[structSpec.size()];
        Arrays.setAll(innerBuffers,
            i -> createHeapCacheBuffer(structSpec.getDataSpec(i), structTraits.getDataTraits(i), index.getChild(i)));
        if (Stream.of(innerBuffers).anyMatch(c -> c == UncachedHeapBuffer.INSTANCE)) {
            // we can either cache all children or none
            return UncachedHeapBuffer.INSTANCE;
        } else {
            return new StructHeapCacheBuffer(innerBuffers);
        }
    }

    /**
     *
     * @param <ARRAY_TYPE>
     */
    public interface HeapCacheBuffer<ARRAY_TYPE> {
        /**
         * @param capacity
         */
        void init(int capacity);

        /**
         * @param index
         * @return
         */
        WriteAccess getAccess(int index);

        /**
         * @return
         */
        ARRAY_TYPE getArray();
    }

    private enum UncachedHeapBuffer implements HeapCacheBuffer<Object[]> {
            INSTANCE;

        private UncachedHeapBufferWriteAccess m_devNullWriteAccess = new UncachedHeapBufferWriteAccess();

        @Override
        public void init(final int capacity) {
            // NOOP
        }

        @Override
        public WriteAccess getAccess(final int index) {
            return m_devNullWriteAccess;
        }

        @Override
        public Object[] getArray() {
            return null;
        }

        private static class UncachedHeapBufferWriteAccess implements WriteAccess {
            @Override
            public void setMissing() {
                // NOOP
            }

            @Override
            public void setFrom(final ReadAccess access) {
                // NOOP
            }

        }
    }

    private static class StringHeapCacheBuffer implements HeapCacheBuffer<String[]> {
        private String[] m_data;

        @Override
        public void init(final int capacity) {
            m_data = new String[capacity];
        }

        @Override
        public WriteAccess getAccess(final int index) {
            return new StringHeapCacheBufferWriteAccess(index);
        }

        @Override
        public String[] getArray() {
            return m_data;
        }

        private class StringHeapCacheBufferWriteAccess implements WriteAccess {
            private int m_index;

            private StringHeapCacheBufferWriteAccess(final int index) {
                m_index = index;
            }

            @Override
            public void setMissing() {
                // NOOP
            }

            @Override
            public void setFrom(final ReadAccess access) {
                m_data[m_index] = ((StringReadAccess)access).getStringValue();
            }
        }
    }

    private static class VarBinaryHeapCacheBuffer implements HeapCacheBuffer<Object[]> {
        private Object[] m_data;

        @Override
        public void init(final int capacity) {
            m_data = new Object[capacity];
        }

        @Override
        public WriteAccess getAccess(final int index) {
            return new VarBinaryHeapCacheBufferWriteAccess(index);
        }

        @Override
        public Object[] getArray() {
            return m_data;
        }

        private class VarBinaryHeapCacheBufferWriteAccess implements WriteAccess {
            private int m_index;

            private VarBinaryHeapCacheBufferWriteAccess(final int index) {
                m_index = index;
            }

            @Override
            public void setMissing() {
                // NOOP
            }

            @Override
            public void setFrom(final ReadAccess access) {
                m_data[m_index] = ((VarBinaryReadAccess)access).getObject();
            }
        }
    }

    private static class StructHeapCacheBuffer implements HeapCacheBuffer<Object[]> {
        private HeapCacheBuffer<?>[] m_innerFactories;

        private StructHeapCacheBuffer(final HeapCacheBuffer<?>[] innerFactories) {
            m_innerFactories = innerFactories;
        }

        @Override
        public void init(final int capacity) {
            Arrays.stream(m_innerFactories).forEach(f -> f.init(capacity));
        }

        @Override
        public WriteAccess getAccess(final int index) {
            return new StructHeapCacheBufferWriteAccess(index);
        }

        @Override
        public Object[] getArray() {
            return Arrays.stream(m_innerFactories).map(f -> f.getArray()).toArray();
        }

        private class StructHeapCacheBufferWriteAccess implements WriteAccess {
            private int m_index;

            private StructHeapCacheBufferWriteAccess(final int index) {
                m_index = index;
            }

            @Override
            public void setMissing() {
                // NOOP
            }

            @Override
            public void setFrom(final ReadAccess access) {
                final StructReadAccess structAccess = (StructReadAccess)access;
                for (int i = 0; i < m_innerFactories.length; i++) {
                    m_innerFactories[i].getAccess(m_index).setFrom(structAccess.getAccess(i));
                }
            }
        }
    }
}
