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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import org.knime.core.columnar.arrow.data.OnHeapBooleanData;
import org.knime.core.columnar.arrow.data.OnHeapByteData;
import org.knime.core.columnar.arrow.data.OnHeapDictEncodedStringData;
import org.knime.core.columnar.arrow.data.OnHeapDictEncodedVarBinaryData;
import org.knime.core.columnar.arrow.data.OnHeapDoubleData;
import org.knime.core.columnar.arrow.data.OnHeapIntData;
import org.knime.core.columnar.arrow.data.OnHeapListData;
import org.knime.core.columnar.arrow.data.OnHeapLongData;
import org.knime.core.columnar.arrow.data.OnHeapStringData5000;
import org.knime.core.columnar.arrow.data.OnHeapStructData;
import org.knime.core.columnar.arrow.data.OnHeapVarBinaryData;
import org.knime.core.columnar.arrow.data.OnHeapVoidData;
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
 * Utility class to map a {@link ColumnarSchema} to an array of {@link ArrowColumnDataFactory}. The factories can be
 * used to create, read or write the Arrow implementations of {@link NullableReadData} and {@link NullableWriteData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class ArrowSchemaMapper implements MapperWithTraits<ArrowColumnDataFactory> {

    private static final ArrowSchemaMapper INSTANCE = new ArrowSchemaMapper();

    private record FactoryWithTraitsKey(ArrowColumnDataFactory factory, DataTraits traits) {
    }

    // There will ever only be a rather limited set of different factories used, so in case of many
    // columns we cache and reuse the factories for other columns
    private final Map<FactoryWithTraitsKey, ArrowColumnDataFactory> m_usedFactories = new ConcurrentHashMap<>();

    private ArrowSchemaMapper() {
        // Singleton and the instance is only used in #map
    }

    /**
     * Map each column of the {@link ColumnarSchema} to the according {@link ArrowColumnDataFactory}. The factory can be
     * used to create, read or write the Arrow implementation of {@link NullableReadData} and {@link NullableWriteData}.
     *
     * @param schema the schema of the column store
     * @return the factories
     */
    static ArrowColumnDataFactory[] map(final ColumnarSchema schema) {
        return IntStream.range(0, schema.numColumns()) //
            .mapToObj(i -> map(schema.getSpec(i), schema.getTraits(i))).toArray(ArrowColumnDataFactory[]::new);
    }

    /**
     * Map a single {@link DataSpec} to the according {@link ArrowColumnDataFactory}. The factory can be used to create,
     * read or write the Arrow implementation of {@link NullableReadData} and {@link NullableWriteData}.
     *
     * @param spec the spec of the column
     * @return the factory
     */
    static ArrowColumnDataFactory map(final DataSpec spec, final DataTraits traits) {
        return spec.accept(INSTANCE, traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final BooleanDataSpec spec, final DataTraits traits) {
        return wrapCached(OnHeapBooleanData.FACTORY, traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final ByteDataSpec spec, final DataTraits traits) {
        return wrapCached(OnHeapByteData.FACTORY, traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final DoubleDataSpec spec, final DataTraits traits) {
        return wrapCached(OnHeapDoubleData.FACTORY, traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final FloatDataSpec spec, final DataTraits traits) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public ArrowColumnDataFactory visit(final IntDataSpec spec, final DataTraits traits) {
        return wrapCached(OnHeapIntData.FACTORY, traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final LongDataSpec spec, final DataTraits traits) {
        return wrapCached(OnHeapLongData.FACTORY, traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final VarBinaryDataSpec spec, final DataTraits traits) {
        if (DictEncodingTrait.isEnabled(traits)) {
            return wrapCached(OnHeapDictEncodedVarBinaryData.factory(traits), traits);
        } else {
            return wrapCached(OnHeapVarBinaryData.FACTORY, traits);
        }
    }

    @Override
    public ArrowColumnDataFactory visit(final VoidDataSpec spec, final DataTraits traits) {
        return wrapCached(OnHeapVoidData.FACTORY, traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final StructDataSpec spec, final StructDataTraits traits) {
        final var innerFactories = new ArrowColumnDataFactory[spec.size()];
        Arrays.setAll(innerFactories, i -> map(spec.getDataSpec(i), traits.getDataTraits(i)));
        return wrapCached(OnHeapStructData.factory(innerFactories), traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final ListDataSpec listDataSpec, final ListDataTraits traits) {
        final ArrowColumnDataFactory inner = ArrowSchemaMapper.map(listDataSpec.getInner(), traits.getInner());
        return wrapCached(OnHeapListData.factory(inner), traits);
    }

    @Override
    public ArrowColumnDataFactory visit(final StringDataSpec spec, final DataTraits traits) {
        if (DictEncodingTrait.isEnabled(traits)) {
            return wrapCached(OnHeapDictEncodedStringData.factory(traits), traits);
        }
        // TODO the caching does not work like that
        return wrapCached(OnHeapStringData5000.FACTORY, traits);
    }

    ArrowColumnDataFactory wrapCached(final ArrowColumnDataFactory factory, final DataTraits traits) {
        return m_usedFactories.computeIfAbsent(new FactoryWithTraitsKey(factory, traits), key -> {
            if (key.traits.hasTrait(LogicalTypeTrait.class) || key.traits.hasTrait(DictEncodingTrait.class)) {
                // TODO add extension type
                return factory;
                //                return new ExtensionArrowColumnDataFactory(key.factory, key.traits);
            }

            return factory;
        });
    }

    static ArrowColumnDataFactory wrap(final ArrowColumnDataFactory factory, final DataTraits traits) {
        return INSTANCE.wrapCached(factory, traits);
    }
}