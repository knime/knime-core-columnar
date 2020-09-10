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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.arrow.data.ArrowBinarySupplementData;
import org.knime.core.columnar.arrow.data.ArrowData;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedStringData;
import org.knime.core.columnar.arrow.data.ArrowDoubleData;
import org.knime.core.columnar.arrow.data.ArrowIntData;
import org.knime.core.columnar.arrow.data.ArrowLongData;
import org.knime.core.columnar.arrow.data.ArrowVarBinaryData;
import org.knime.core.columnar.arrow.data.ArrowVarCharData;
import org.knime.core.columnar.data.BinarySupplData;
import org.knime.core.columnar.data.BinarySupplData.BinarySupplDataSpec;
import org.knime.core.columnar.data.DoubleData;
import org.knime.core.columnar.data.DoubleData.DoubleDataSpec;
import org.knime.core.columnar.data.IntData;
import org.knime.core.columnar.data.IntData.IntDataSpec;
import org.knime.core.columnar.data.LongData;
import org.knime.core.columnar.data.LongData.LongDataSpec;
import org.knime.core.columnar.data.StringData;
import org.knime.core.columnar.data.StringData.StringDataSpec;
import org.knime.core.columnar.data.VarBinaryData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryDataSpec;

final class ArrowSchemaMapperV0 implements ArrowSchemaMapper {

    private static long m_id;

    private static long nextUniqueId() {
        return m_id++;
    }

    @Override
    public ArrowColumnDataSpec<?>[] map(final ColumnStoreSchema schema) {
        final ArrowColumnDataSpec<?>[] arrowSchema = new ArrowColumnDataSpec<?>[schema.getNumColumns()];
        for (int i = 0; i < schema.getNumColumns(); i++) {
            arrowSchema[i] = getMapping(schema.getColumnDataSpec(i));
        }
        return arrowSchema;
    }

    // TODO different pattern
    private static final ArrowColumnDataSpec<?> getMapping(final ColumnDataSpec<?> spec) {
        if (spec instanceof DoubleDataSpec) {
            return new ArrowDoubleDataSpec();
        } else if (spec instanceof StringDataSpec) {
            return new ArrowStringDataSpec((StringDataSpec)spec);
        } else if (spec instanceof IntDataSpec) {
            return new ArrowIntDataSpec();
        } else if (spec instanceof BinarySupplDataSpec) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            ArrowBinarySupplDataSpec<?> arrowSpec = new ArrowBinarySupplDataSpec((BinarySupplDataSpec<?>)spec);
            return arrowSpec;
        } else if (spec instanceof LongDataSpec) {
            return new ArrowLongDataSpec();
        } else if (spec instanceof VarBinaryDataSpec) {
            return new ArrowVarBinaryDataSpec();
        }
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");

    }

    static class ArrowLongDataSpec implements ArrowColumnDataSpec<LongData> {
        @Override
        public LongData createEmpty(final BufferAllocator allocator) {
            return new ArrowLongData(allocator);
        }

        @Override
        public LongData wrap(final FieldVector vector, final DictionaryProvider provider) {
            return new ArrowLongData((BigIntVector)vector);
        }
    }

    static class ArrowDoubleDataSpec implements ArrowColumnDataSpec<DoubleData> {
        @Override
        public DoubleData createEmpty(final BufferAllocator allocator) {
            return new ArrowDoubleData(allocator);
        }

        @Override
        public DoubleData wrap(final FieldVector vector, final DictionaryProvider provider) {
            return new ArrowDoubleData((Float8Vector)vector);
        }
    }

    static class ArrowIntDataSpec implements ArrowColumnDataSpec<IntData> {
        @Override
        public IntData createEmpty(final BufferAllocator allocator) {
            return new ArrowIntData(allocator);
        }

        @Override
        public IntData wrap(final FieldVector vector, final DictionaryProvider provider) {
            return new ArrowIntData((IntVector)vector);
        }
    }

    static class ArrowVarBinaryDataSpec implements ArrowColumnDataSpec<VarBinaryData> {

        @Override
        public VarBinaryData createEmpty(final BufferAllocator allocator) {
            return new ArrowVarBinaryData(allocator);
        }

        @Override
        public VarBinaryData wrap(final FieldVector vector, final DictionaryProvider provider) {
            return new ArrowVarBinaryData((VarBinaryVector)vector);
        }
    }

    static class ArrowBinarySupplDataSpec<C extends ArrowData<?>> implements ArrowColumnDataSpec<BinarySupplData<C>> {

        private final BinarySupplDataSpec<C> m_spec;

        private ArrowColumnDataSpec<C> m_arrowChunkSpec;

        @SuppressWarnings("unchecked")
        public ArrowBinarySupplDataSpec(final BinarySupplDataSpec<C> spec) {
            m_spec = spec;
            m_arrowChunkSpec = (ArrowColumnDataSpec<C>)ArrowSchemaMapperV0.getMapping(m_spec.getChildSpec());
        }

        @Override
        public BinarySupplData<C> createEmpty(final BufferAllocator allocator) {
            return new ArrowBinarySupplementData<C>(allocator, m_arrowChunkSpec.createEmpty(allocator));
        }

        @Override
        public BinarySupplData<C> wrap(final FieldVector vector, final DictionaryProvider provider) {
            @SuppressWarnings("unchecked")
            final ArrowColumnDataSpec<C> arrowSpec = (ArrowColumnDataSpec<C>)getMapping(m_spec.getChildSpec());
            final C wrapped = arrowSpec.wrap((FieldVector)((StructVector)vector).getChildByOrdinal(0), provider);
            return new ArrowBinarySupplementData<>((StructVector)vector, wrapped);
        }
    }

    static class ArrowStringDataSpec implements ArrowColumnDataSpec<StringData> {

        private final StringDataSpec m_spec;

        private final long id = nextUniqueId();

        public ArrowStringDataSpec(final StringDataSpec spec) {
            m_spec = spec;
        }

        @Override
        public StringData createEmpty(final BufferAllocator allocator) {
            if (m_spec.isDictEnabled()) {
                return new ArrowDictEncodedStringData(allocator, id);
            } else {
                return new ArrowVarCharData(allocator);
            }
        }

        @Override
        public StringData wrap(final FieldVector vector, final DictionaryProvider provider) {
            if (m_spec.isDictEnabled()) {
                return new ArrowDictEncodedStringData((IntVector)vector, (VarCharVector)provider
                    .lookup(vector.getField().getFieldType().getDictionary().getId()).getVector());
            } else {
                return new ArrowVarCharData((VarCharVector)vector);
            }
        }
    }
}
