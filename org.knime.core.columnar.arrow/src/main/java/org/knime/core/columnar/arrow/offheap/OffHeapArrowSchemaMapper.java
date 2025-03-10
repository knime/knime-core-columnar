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
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.extensiontypes.ExtensionTypes;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowBooleanData.ArrowBooleanDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowByteData.ArrowByteDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowDictEncodedStringData.ArrowDictEncodedStringDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowFloatData.ArrowFloatDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowListData.ArrowListDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowLongData.ArrowLongDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowReadData;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStringData.ArrowStringDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowVoidData.ArrowVoidDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpec.MapperWithTraits;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;
import org.knime.core.table.schema.traits.LogicalTypeTrait;
import org.knime.core.table.schema.traits.StructDataTraits;

/**
 * Utility class to map a {@link ColumnarSchema} to an array of {@link OffHeapArrowColumnDataFactory}. The factories can be
 * used to create, read or write the Arrow implementations of {@link NullableReadData} and {@link NullableWriteData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class OffHeapArrowSchemaMapper implements MapperWithTraits<OffHeapArrowColumnDataFactory> {

    private static final OffHeapArrowSchemaMapper INSTANCE = new OffHeapArrowSchemaMapper();

    private record FactoryWithTraitsKey(OffHeapArrowColumnDataFactory factory, DataTraits traits) {
    }

    // There will ever only be a rather limited set of different factories used, so in case of many
    // columns we cache and reuse the factories for other columns
    private final Map<FactoryWithTraitsKey, OffHeapArrowColumnDataFactory> m_usedFactories = new ConcurrentHashMap<>();

    private OffHeapArrowSchemaMapper() {
        // Singleton and the instance is only used in #map
    }

    /**
     * Map each column of the {@link ColumnarSchema} to the according {@link OffHeapArrowColumnDataFactory}. The factory can be
     * used to create, read or write the Arrow implementation of {@link NullableReadData} and {@link NullableWriteData}.
     *
     * @param schema the schema of the column store
     * @return the factories
     */
    static OffHeapArrowColumnDataFactory[] map(final ColumnarSchema schema) {
        return IntStream.range(0, schema.numColumns()) //
            .mapToObj(i -> map(schema.getSpec(i), schema.getTraits(i))).toArray(OffHeapArrowColumnDataFactory[]::new);
    }

    /**
     * Map a single {@link DataSpec} to the according {@link OffHeapArrowColumnDataFactory}. The factory can be used to create,
     * read or write the Arrow implementation of {@link NullableReadData} and {@link NullableWriteData}.
     *
     * @param spec the spec of the column
     * @return the factory
     */
    static OffHeapArrowColumnDataFactory map(final DataSpec spec, final DataTraits traits) {
        return spec.accept(INSTANCE, traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final BooleanDataSpec spec, final DataTraits traits) {
        return wrapCached(ArrowBooleanDataFactory.INSTANCE, traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final ByteDataSpec spec, final DataTraits traits) {
        return wrapCached(ArrowByteDataFactory.INSTANCE, traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final DoubleDataSpec spec, final DataTraits traits) {
        return wrapCached(ArrowDoubleDataFactory.INSTANCE, traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final FloatDataSpec spec, final DataTraits traits) {
        return wrapCached(ArrowFloatDataFactory.INSTANCE, traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final IntDataSpec spec, final DataTraits traits) {
        return wrapCached(ArrowIntDataFactory.INSTANCE, traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final LongDataSpec spec, final DataTraits traits) {
        return wrapCached(ArrowLongDataFactory.INSTANCE, traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final VarBinaryDataSpec spec, final DataTraits traits) {
        if (DictEncodingTrait.isEnabled(traits)) {
            return wrapCached(new ArrowDictEncodedVarBinaryDataFactory(traits), traits);
        } else {
            return wrapCached(ArrowVarBinaryDataFactory.INSTANCE, traits);
        }
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final VoidDataSpec spec, final DataTraits traits) {
        return wrapCached(ArrowVoidDataFactory.INSTANCE, traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final StructDataSpec spec, final StructDataTraits traits) {
        final var innerFactories = new OffHeapArrowColumnDataFactory[spec.size()];
        Arrays.setAll(innerFactories, i -> map(spec.getDataSpec(i), traits.getDataTraits(i)));
        return wrapCached(new ArrowStructDataFactory(innerFactories), traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final ListDataSpec listDataSpec, final ListDataTraits traits) {
        final OffHeapArrowColumnDataFactory inner = OffHeapArrowSchemaMapper.map(listDataSpec.getInner(), traits.getInner());
        return wrapCached(new ArrowListDataFactory(inner), traits);
    }

    @Override
    public OffHeapArrowColumnDataFactory visit(final StringDataSpec spec, final DataTraits traits) {
        if (DictEncodingTrait.isEnabled(traits)) {
            return wrapCached(new ArrowDictEncodedStringDataFactory(traits), traits);
        }
        return wrapCached(ArrowStringDataFactory.INSTANCE, traits);
    }

    OffHeapArrowColumnDataFactory wrapCached(final OffHeapArrowColumnDataFactory factory, final DataTraits traits) {
        return m_usedFactories.computeIfAbsent(new FactoryWithTraitsKey(factory, traits), key -> {
            if (key.traits.hasTrait(LogicalTypeTrait.class) || key.traits.hasTrait(DictEncodingTrait.class)) {
                return new ExtensionArrowColumnDataFactory(key.factory, key.traits);
            }

            return factory;
        });
    }

    static OffHeapArrowColumnDataFactory wrap(final OffHeapArrowColumnDataFactory factory, final DataTraits traits) {
        return INSTANCE.wrapCached(factory, traits);
    }

    static final class ExtensionArrowColumnDataFactory implements OffHeapArrowColumnDataFactory {

        private final OffHeapArrowColumnDataFactory m_delegate;

        private final DataTraits m_traits;

        private final ArrowColumnDataFactoryVersion m_version;

        private static final int CURRENT_VERSION = 0;

        ExtensionArrowColumnDataFactory(final OffHeapArrowColumnDataFactory delegate, final DataTraits traits) {
            m_delegate = delegate;
            m_traits = traits;
            m_version = ArrowColumnDataFactoryVersion.version(CURRENT_VERSION, m_delegate.getVersion());
        }

        OffHeapArrowColumnDataFactory getDelegate() {
            return m_delegate;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            var storageField = m_delegate.getField(name, dictionaryIdSupplier);
            return ExtensionTypes.wrapInExtensionTypeIfNecessary(storageField, m_traits);
        }

        @Override
        public OffHeapArrowWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            return m_delegate.createWrite(vector, dictionaryIdSupplier, allocator, capacity);
        }

        @Override
        public OffHeapArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            return m_delegate.createRead(vector, nullCount, provider, version.getChildVersion(0));
        }

        @Override
        public FieldVector getVector(final NullableReadData data) {
            return m_delegate.getVector(data);
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            return m_delegate.getDictionaries(data);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return m_version;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof ExtensionArrowColumnDataFactory) {
                var other = (ExtensionArrowColumnDataFactory)obj;
                return m_traits.equals(other.m_traits) && m_delegate.equals(other.m_delegate);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_traits, m_delegate, m_version);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + ".v" + CURRENT_VERSION + "[" + m_delegate + "]";
        }

        @Override
        public int initialNumBytesPerElement() {
            return m_delegate.initialNumBytesPerElement();
        }
    }
}