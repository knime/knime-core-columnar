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

import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedStringData;
import org.knime.core.columnar.arrow.data.ArrowDoubleData;
import org.knime.core.columnar.arrow.data.ArrowIntData;
import org.knime.core.columnar.arrow.data.ArrowLongData;
import org.knime.core.columnar.arrow.data.ArrowVarBinaryData;
import org.knime.core.columnar.arrow.data.ArrowVarCharData;
import org.knime.core.columnar.data.BooleanData.BooleanDataSpec;
import org.knime.core.columnar.data.ByteData.ByteDataSpec;
import org.knime.core.columnar.data.ColumnDataSpec.Mapper;
import org.knime.core.columnar.data.DoubleData.DoubleDataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.DurationData.DurationDataSpec;
import org.knime.core.columnar.data.FloatData.FloatDataSpec;
import org.knime.core.columnar.data.IntData.IntDataSpec;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.LocalDateData.LocalDateDataSpec;
import org.knime.core.columnar.data.LocalDateTimeData.LocalDateTimeDataSpec;
import org.knime.core.columnar.data.LocalTimeData.LocalTimeDataSpec;
import org.knime.core.columnar.data.LongData.LongDataSpec;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.PeriodData.PeriodDataSpec;
import org.knime.core.columnar.data.StringData.StringDataSpec;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryDataSpec;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.VoidData.VoidDataSpec;
import org.knime.core.columnar.data.VoidData.VoidReadData;
import org.knime.core.columnar.data.VoidData.VoidWriteData;
import org.knime.core.columnar.store.ColumnStoreSchema;

final class ArrowSchemaMapperV0 implements ArrowSchemaMapper, Mapper<ArrowColumnDataSpec<?, ?>> {

    static final ArrowSchemaMapperV0 INSTANCE = new ArrowSchemaMapperV0();

    private ArrowSchemaMapperV0() {
    }

    private static long m_id;

    private static long nextUniqueId() {
        return m_id++;
    }

    @Override
    public ArrowColumnDataSpec<?, ?>[] map(final ColumnStoreSchema schema) {
        return IntStream.range(0, schema.getNumColumns()).mapToObj(schema::getColumnDataSpec)
            .map(spec -> spec.accept(this)).toArray(ArrowColumnDataSpec<?, ?>[]::new);
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final BooleanDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final ByteDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowDoubleDataSpec visit(final DoubleDataSpec spec) {
        return new ArrowDoubleDataSpec();
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final DurationDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final FloatDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowIntDataSpec visit(final IntDataSpec spec) {
        return new ArrowIntDataSpec();
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final LocalDateDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final LocalDateTimeDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final LocalTimeDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowLongDataSpec visit(final LongDataSpec spec) {
        return new ArrowLongDataSpec();
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final PeriodDataSpec spec) {
        throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");
    }

    @Override
    public ArrowStringDataSpec visit(final StringDataSpec spec) {
        return new ArrowStringDataSpec(spec);
    }

    @Override
    public ArrowVarBinaryDataSpec visit(final VarBinaryDataSpec spec) {
        return new ArrowVarBinaryDataSpec();
    }

    @Override
    public ArrowColumnDataSpec<?, ?> visit(final VoidDataSpec spec) {
        return new ArrowColumnDataSpec<VoidWriteData, VoidReadData>() {

            @Override
            public VoidWriteData createEmpty(final BufferAllocator allocator, final int capacity) {
                return new ArrowVoidData(capacity);
            }

            @Override
            public VoidReadData wrap(final FieldVector vector, final DictionaryProvider provider) {
                return new ArrowVoidData((NullVector)vector);
            }
        };
    }

    static class ArrowLongDataSpec implements ArrowColumnDataSpec<LongWriteData, LongReadData> {
        @Override
        public ArrowLongData createEmpty(final BufferAllocator allocator, final int capacity) {
            return new ArrowLongData(allocator, capacity);
        }

        @Override
        public ArrowLongData wrap(final FieldVector vector, final DictionaryProvider provider) {
            return new ArrowLongData((BigIntVector)vector);
        }
    }

    static class ArrowDoubleDataSpec implements ArrowColumnDataSpec<DoubleWriteData, DoubleReadData> {
        @Override
        public ArrowDoubleData createEmpty(final BufferAllocator allocator, final int capacity) {
            return new ArrowDoubleData(allocator, capacity);
        }

        @Override
        public ArrowDoubleData wrap(final FieldVector vector, final DictionaryProvider provider) {
            return new ArrowDoubleData((Float8Vector)vector);
        }
    }

    static class ArrowIntDataSpec implements ArrowColumnDataSpec<IntWriteData, IntReadData> {
        @Override
        public ArrowIntData createEmpty(final BufferAllocator allocator, final int capacity) {
            return new ArrowIntData(allocator, capacity);
        }

        @Override
        public ArrowIntData wrap(final FieldVector vector, final DictionaryProvider provider) {
            return new ArrowIntData((IntVector)vector);
        }
    }

    static class ArrowVarBinaryDataSpec implements ArrowColumnDataSpec<VarBinaryWriteData, VarBinaryReadData> {

        @Override
        public ArrowVarBinaryData createEmpty(final BufferAllocator allocator, final int capacity) {
            return new ArrowVarBinaryData(allocator, capacity);
        }

        @Override
        public ArrowVarBinaryData wrap(final FieldVector vector, final DictionaryProvider provider) {
            return new ArrowVarBinaryData((VarBinaryVector)vector);
        }
    }

    static class ArrowStringDataSpec implements ArrowColumnDataSpec<StringWriteData, StringReadData> {

        private final StringDataSpec m_spec;

        private final long id = nextUniqueId();

        public ArrowStringDataSpec(final StringDataSpec spec) {
            m_spec = spec;
        }

        @Override
        public StringWriteData createEmpty(final BufferAllocator allocator, final int capacity) {
            if (m_spec.isDictEnabled()) {
                return new ArrowDictEncodedStringData(allocator, id, capacity);
            } else {
                return new ArrowVarCharData(allocator, capacity);
            }
        }

        @Override
        public StringReadData wrap(final FieldVector vector, final DictionaryProvider provider) {
            if (m_spec.isDictEnabled()) {
                return new ArrowDictEncodedStringData((IntVector)vector, (VarCharVector)provider
                    .lookup(vector.getField().getFieldType().getDictionary().getId()).getVector());
            } else {
                return new ArrowVarCharData((VarCharVector)vector);
            }
        }
    }

}