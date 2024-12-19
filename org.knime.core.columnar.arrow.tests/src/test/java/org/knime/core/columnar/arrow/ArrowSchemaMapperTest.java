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
 *   Oct 2, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.knime.core.table.schema.DataSpecs.DICT_ENCODING;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.schema.DataSpecs.VARBINARY;

import java.util.Arrays;

import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowSchemaMapper.ExtensionArrowColumnDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapBooleanData.OnHeapBooleanDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapByteData.OnHeapByteDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapDictEncodedStringData.OnHeapDictEncodedStringDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapDictEncodedVarBinaryData.OnHeapDictEncodedVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapDoubleData.OnHeapDoubleDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapFloatData.OnHeapFloatDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapListData.OnHeapListDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapLongData.OnHeapLongDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapStringData5000.OnHeapStringDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapStructData.OnHeapStructDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapVarBinaryData.OnHeapVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapVoidData.OnHeapVoidDataFactory;
import org.knime.core.columnar.arrow.data.old.ArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.data.old.ArrowListData.ArrowListDataFactory;
import org.knime.core.columnar.arrow.data.old.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DefaultListDataTraits;
import org.knime.core.table.schema.traits.DefaultStructDataTraits;
import org.knime.core.table.schema.traits.LogicalTypeTrait;

/**
 * Test the ArrowSchemaMapper.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class ArrowSchemaMapperTest {

    /** Test mapping double specs to a {@link ArrowDoubleDataFactory} */
    @Test
    public void testMapDoubleSpec() {
        testMapSingleSpec(DataSpec.doubleSpec(), OnHeapDoubleDataFactory.INSTANCE);
    }

    /** Test mapping int specs to a {@link OnHeapIntDataFactory} */
    @Test
    public void testMapIntSpec() {
        testMapSingleSpec(DataSpec.intSpec(), OnHeapIntDataFactory.INSTANCE);
    }

    /** Test mapping long specs to a {@link OnHeapLongDataFactory} */
    @Test
    public void testMapLongSpec() {
        testMapSingleSpec(DataSpec.longSpec(), OnHeapLongDataFactory.INSTANCE);
    }

    /** Test mapping float specs to a {@link OnHeapFloatDataFactory} */
    @Test
    public void testMapFloatSpec() {
        testMapSingleSpec(DataSpec.floatSpec(), OnHeapFloatDataFactory.INSTANCE);
    }

    /** Test mapping boolean specs to a {@link OnHeapBooleanDataFactory} */
    @Test
    public void testMapBooleanSpec() {
        testMapSingleSpec(DataSpec.booleanSpec(), OnHeapBooleanDataFactory.INSTANCE);
    }

    /** Test mapping byte specs to a {@link OnHeapByteDataFactory} */
    @Test
    public void testMapByteSpec() {
        testMapSingleSpec(DataSpec.byteSpec(), OnHeapByteDataFactory.INSTANCE);
    }

    /** Test mapping var binary specs to a {@link OnHeapVarBinaryDataFactory} */
    @Test
    public void testMapVarBinarySpec() {
        testMapSingleSpec(DataSpec.varBinarySpec(), OnHeapVarBinaryDataFactory.INSTANCE);
    }

    /** Test mapping void specs to a {@link OnHeapVoidDataFactory} */
    @Test
    public void testMapVoidSpec() {
        testMapSingleSpec(DataSpec.voidSpec(), OnHeapVoidDataFactory.INSTANCE);
    }

    /** Test mapping DictEncodedStringData specs to a {@link OnHeapDictEncodedStringDataFactory} */
    @Test
    public void testMapDictEncodedStringDataSpec() {
        testMapSchema(ColumnarSchema.of(STRING(DICT_ENCODING)),
            new OnHeapDictEncodedStringDataFactory(createDictEncodingTraits(KeyType.LONG_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link OnHeapDictEncodedStringDataFactory} */
    @Test
    public void testMapDictEncodedStringDataSpec_ByteKey() {
        testMapSchema(ColumnarSchema.of(STRING(DICT_ENCODING(KeyType.BYTE_KEY))),
            new OnHeapDictEncodedStringDataFactory(createDictEncodingTraits(KeyType.BYTE_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link OnHeapDictEncodedStringDataFactory} */
    @Test
    public void testMapDictEncodedStringDataSpec_IntKey() {
        testMapSchema(ColumnarSchema.of(STRING(DICT_ENCODING(KeyType.INT_KEY))),
            new OnHeapDictEncodedStringDataFactory(createDictEncodingTraits(KeyType.INT_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link OnHeapDictEncodedStringDataFactory} */
    @Test
    public void testMapDictEncodedStringDataSpec_LongKey() {
        testMapSchema(ColumnarSchema.of(STRING(DICT_ENCODING(KeyType.LONG_KEY))),
            new OnHeapDictEncodedStringDataFactory(createDictEncodingTraits(KeyType.LONG_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link OnHeapDictEncodedVarBinaryDataFactory} */
    @Test
    public void testMapDictEncodedVarBinaryDataSpec() {
        testMapSchema(ColumnarSchema.of(VARBINARY(DICT_ENCODING)),
            new OnHeapDictEncodedVarBinaryDataFactory(createDictEncodingTraits(KeyType.LONG_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link OnHeapDictEncodedVarBinaryDataFactory} */
    @Test
    public void testMapDictEncodedVarBinaryDataSpec_ByteKey() {
        testMapSchema(ColumnarSchema.of(VARBINARY(DICT_ENCODING(KeyType.BYTE_KEY))),
            new OnHeapDictEncodedVarBinaryDataFactory(createDictEncodingTraits(KeyType.BYTE_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link OnHeapDictEncodedVarBinaryDataFactory} */
    @Test
    public void testMapDictEncodedVarBinaryDataSpec_IntKey() {
        testMapSchema(ColumnarSchema.of(VARBINARY(DICT_ENCODING(KeyType.INT_KEY))),
            new OnHeapDictEncodedVarBinaryDataFactory(createDictEncodingTraits(KeyType.INT_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link OnHeapDictEncodedVarBinaryDataFactory} */
    @Test
    public void testMapDictEncodedVarBinaryDataSpec_LongKey() {
        testMapSchema(ColumnarSchema.of(VARBINARY(DICT_ENCODING(KeyType.LONG_KEY))),
            new OnHeapDictEncodedVarBinaryDataFactory(createDictEncodingTraits(KeyType.LONG_KEY)));
    }

    /** Test mapping String specs to a {@link OnHeapStringDataFactory} */
    @Test
    public void testMapStringSpec() {
        testMapSingleSpec(DataSpec.stringSpec(), OnHeapStringDataFactory.INSTANCE);
    }

    /** Test mapping void specs to a {@link ArrowStructDataFactory} */
    @Test
    public void testMapStructSpec() {
        // Simple
        StructDataSpec spec = new StructDataSpec(DataSpec.doubleSpec(), DataSpec.intSpec());
        testMapSingleSpec(spec,
            new OnHeapStructDataFactory(OnHeapDoubleDataFactory.INSTANCE, OnHeapIntDataFactory.INSTANCE));

        // Complex
        StructDataSpec inner = new StructDataSpec(DataSpec.doubleSpec(), DataSpec.longSpec());
        StructDataSpec outer = new StructDataSpec(DataSpec.stringSpec(), DataSpec.intSpec(), inner);
        testMapSingleSpec(outer, new OnHeapStructDataFactory(//
            OnHeapStringDataFactory.INSTANCE, //
            OnHeapIntDataFactory.INSTANCE, //
            new OnHeapStructDataFactory(//
                OnHeapDoubleDataFactory.INSTANCE, //
                OnHeapLongDataFactory.INSTANCE)));
    }

    /** Test mapping list specs to a {@link ArrowListDataFactory} */
    @Test
    public void testMapListSpec() {
        // Simple
        ListDataSpec spec = new ListDataSpec(DataSpec.doubleSpec());
        testMapSingleSpec(spec, new OnHeapListDataFactory(OnHeapDoubleDataFactory.INSTANCE));

        // Complex
        StructDataSpec inner = new StructDataSpec(DataSpec.stringSpec(), DataSpec.doubleSpec());
        ListDataSpec middle = new ListDataSpec(inner);
        ListDataSpec outer = new ListDataSpec(middle);
        testMapSingleSpec(outer, new OnHeapListDataFactory(//
            new OnHeapListDataFactory(//
                new OnHeapStructDataFactory(//
                    OnHeapStringDataFactory.INSTANCE, OnHeapDoubleDataFactory.INSTANCE))));
    }

    // TODO test for other specs when implemented

    /** Test mapping multiple columns of different specs. */
    @Test
    public void testMappingMultipleColumns() {
        final var specs = new DataSpec[]{//
            DataSpec.doubleSpec(), //
            DataSpec.longSpec(), //
            DataSpec.doubleSpec(), //
            DataSpec.intSpec() //
        };
        final var traits = new DataTraits[]{//
            new DefaultDataTraits(new LogicalTypeTrait("hello")), //
            DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY};

        final ColumnarSchema schema = new DefaultColumnarSchema(specs, traits);
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(schema);
        assertEquals(4, factories.length);
        assertSame(OnHeapDoubleDataFactory.INSTANCE, unwrap(factories[0]));
        assertSame(OnHeapLongDataFactory.INSTANCE, unwrap(factories[1]));
        assertSame(OnHeapDoubleDataFactory.INSTANCE, unwrap(factories[2]));
        assertSame(OnHeapIntDataFactory.INSTANCE, unwrap(factories[3]));
    }

    /** Test mapping a single column of the given spec. */
    private static void testMapSingleSpec(final DataSpec spec, final ArrowColumnDataFactory expectedFactory) {
        var traits = createEmptyTraitsForSpec(spec);
        final ColumnarSchema schema = new DefaultColumnarSchema(spec, traits);
        testMapSchema(schema, expectedFactory);
    }

    private static void testMapSchema(final ColumnarSchema schema, final ArrowColumnDataFactory expectedFactory) {
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(schema);
        assertEquals(1, factories.length);
        var factory = factories[0];
        assertEquals(expectedFactory, unwrap(factory));
    }

    private static DataTraits createEmptyTraitsForSpec(final DataSpec spec) {
        if (spec instanceof StructDataSpec) {
            var structSpec = (StructDataSpec)spec;
            final DataTraits[] dataTraits = new DataTraits[structSpec.size()];
            Arrays.setAll(dataTraits, i -> createEmptyTraitsForSpec(structSpec.getDataSpec(i)));
            return new DefaultStructDataTraits(dataTraits);
        } else if (spec instanceof ListDataSpec) {
            var innerTraits = createEmptyTraitsForSpec(((ListDataSpec)spec).getInner());
            return new DefaultListDataTraits(innerTraits);
        }
        return DefaultDataTraits.EMPTY;
    }

    private static DataTraits createDictEncodingTraits(final KeyType keyType) {
        return new DefaultDataTraits(new DataTrait.DictEncodingTrait(keyType));
    }

    private static ArrowColumnDataFactory unwrap(final ArrowColumnDataFactory factory) {
        if (factory instanceof ExtensionArrowColumnDataFactory) {
            return ((ExtensionArrowColumnDataFactory)factory).getDelegate();
        }
        return factory;
    }
}
