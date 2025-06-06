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
package org.knime.core.columnar.arrow.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.knime.core.table.schema.DataSpecs.DICT_ENCODING;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.LIST;
import static org.knime.core.table.schema.DataSpecs.LOGICAL_TYPE;
import static org.knime.core.table.schema.DataSpecs.LONG;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.schema.DataSpecs.STRUCT;
import static org.knime.core.table.schema.DataSpecs.VARBINARY;

import java.util.Arrays;

import org.junit.Test;
import org.knime.core.columnar.arrow.offheap.OffHeapArrowSchemaMapper.ExtensionArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowBooleanData.ArrowBooleanDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowByteData.ArrowByteDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowDictEncodedStringData.ArrowDictEncodedStringDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowFloatData.ArrowFloatDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowListData.ArrowListDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowLongData.ArrowLongDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStringData.ArrowStringDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowVoidData.ArrowVoidDataFactory;
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
public class OffHeapArrowSchemaMapperTest {

    /** Test mapping double specs to a {@link ArrowDoubleDataFactory} */
    @Test
    public void testMapDoubleSpec() {
        testMapSingleSpec(DataSpec.doubleSpec(), ArrowDoubleDataFactory.INSTANCE);
    }

    /** Test mapping int specs to a {@link ArrowIntDataFactory} */
    @Test
    public void testMapIntSpec() {
        testMapSingleSpec(DataSpec.intSpec(), ArrowIntDataFactory.INSTANCE);
    }

    /** Test mapping long specs to a {@link ArrowLongDataFactory} */
    @Test
    public void testMapLongSpec() {
        testMapSingleSpec(DataSpec.longSpec(), ArrowLongDataFactory.INSTANCE);
    }

    /** Test mapping float specs to a {@link ArrowFloatDataFactory} */
    @Test
    public void testMapFloatSpec() {
        testMapSingleSpec(DataSpec.floatSpec(), ArrowFloatDataFactory.INSTANCE);
    }

    /** Test mapping boolean specs to a {@link ArrowBooleanDataFactory} */
    @Test
    public void testMapBooleanSpec() {
        testMapSingleSpec(DataSpec.booleanSpec(), ArrowBooleanDataFactory.INSTANCE);
    }

    /** Test mapping byte specs to a {@link ArrowByteDataFactory} */
    @Test
    public void testMapByteSpec() {
        testMapSingleSpec(DataSpec.byteSpec(), ArrowByteDataFactory.INSTANCE);
    }

    /** Test mapping var binary specs to a {@link ArrowVarBinaryDataFactory} */
    @Test
    public void testMapVarBinarySpec() {
        testMapSingleSpec(DataSpec.varBinarySpec(), ArrowVarBinaryDataFactory.INSTANCE);
    }

    /** Test mapping void specs to a {@link ArrowVoidDataFactory} */
    @Test
    public void testMapVoidSpec() {
        testMapSingleSpec(DataSpec.voidSpec(), ArrowVoidDataFactory.INSTANCE);
    }

    /** Test mapping DictEncodedStringData specs to a {@link ArrowDictEncodedStringDataFactory} */
    @Test
    public void testMapDictEncodedStringDataSpec() {
        testMapSchema(ColumnarSchema.of(STRING(DICT_ENCODING)),
            new ArrowDictEncodedStringDataFactory(createDictEncodingTraits(KeyType.LONG_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link ArrowDictEncodedStringDataFactory} */
    @Test
    public void testMapDictEncodedStringDataSpec_ByteKey() {
        testMapSchema(ColumnarSchema.of(STRING(DICT_ENCODING(KeyType.BYTE_KEY))),
            new ArrowDictEncodedStringDataFactory(createDictEncodingTraits(KeyType.BYTE_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link ArrowDictEncodedStringDataFactory} */
    @Test
    public void testMapDictEncodedStringDataSpec_IntKey() {
        testMapSchema(ColumnarSchema.of(STRING(DICT_ENCODING(KeyType.INT_KEY))),
            new ArrowDictEncodedStringDataFactory(createDictEncodingTraits(KeyType.INT_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link ArrowDictEncodedStringDataFactory} */
    @Test
    public void testMapDictEncodedStringDataSpec_LongKey() {
        testMapSchema(ColumnarSchema.of(STRING(DICT_ENCODING(KeyType.LONG_KEY))),
            new ArrowDictEncodedStringDataFactory(createDictEncodingTraits(KeyType.LONG_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link ArrowDictEncodedVarBinaryDataFactory} */
    @Test
    public void testMapDictEncodedVarBinaryDataSpec() {
        testMapSchema(ColumnarSchema.of(VARBINARY(DICT_ENCODING)),
            new ArrowDictEncodedVarBinaryDataFactory(createDictEncodingTraits(KeyType.LONG_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link ArrowDictEncodedVarBinaryDataFactory} */
    @Test
    public void testMapDictEncodedVarBinaryDataSpec_ByteKey() {
        testMapSchema(ColumnarSchema.of(VARBINARY(DICT_ENCODING(KeyType.BYTE_KEY))),
            new ArrowDictEncodedVarBinaryDataFactory(createDictEncodingTraits(KeyType.BYTE_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link ArrowDictEncodedVarBinaryDataFactory} */
    @Test
    public void testMapDictEncodedVarBinaryDataSpec_IntKey() {
        testMapSchema(ColumnarSchema.of(VARBINARY(DICT_ENCODING(KeyType.INT_KEY))),
            new ArrowDictEncodedVarBinaryDataFactory(createDictEncodingTraits(KeyType.INT_KEY)));
    }

    /** Test mapping DictEncodedStringData specs to a {@link ArrowDictEncodedVarBinaryDataFactory} */
    @Test
    public void testMapDictEncodedVarBinaryDataSpec_LongKey() {
        testMapSchema(ColumnarSchema.of(VARBINARY(DICT_ENCODING(KeyType.LONG_KEY))),
            new ArrowDictEncodedVarBinaryDataFactory(createDictEncodingTraits(KeyType.LONG_KEY)));
    }

    /** Test mapping String specs to a {@link ArrowStringDataFactory} */
    @Test
    public void testMapStringSpec() {
        testMapSingleSpec(DataSpec.stringSpec(), ArrowStringDataFactory.INSTANCE);
    }

    /** Test mapping void specs to a {@link ArrowStructDataFactory} */
    @Test
    public void testMapStructSpec() {
        // Simple
        StructDataSpec spec = new StructDataSpec(DataSpec.doubleSpec(), DataSpec.intSpec());
        testMapSingleSpec(spec,
            new ArrowStructDataFactory(ArrowDoubleDataFactory.INSTANCE, ArrowIntDataFactory.INSTANCE));

        // Complex
        StructDataSpec inner = new StructDataSpec(DataSpec.doubleSpec(), DataSpec.longSpec());
        StructDataSpec outer = new StructDataSpec(DataSpec.stringSpec(), DataSpec.intSpec(), inner);
        testMapSingleSpec(outer, new ArrowStructDataFactory(//
            ArrowStringDataFactory.INSTANCE, //
            ArrowIntDataFactory.INSTANCE, //
            new ArrowStructDataFactory(//
                ArrowDoubleDataFactory.INSTANCE, //
                ArrowLongDataFactory.INSTANCE)));
    }

    /** Test mapping void specs to a {@link ArrowStructDataFactory} */
    @Test
    public void testMapStructSpecWithLogicalType() {
        // Simple
        final var schema = ColumnarSchema.of(LIST.of(DOUBLE(LOGICAL_TYPE("foo"))));

        testMapSchema(schema, new ArrowListDataFactory(//
            new ExtensionArrowColumnDataFactory(ArrowDoubleDataFactory.INSTANCE,
                new DefaultDataTraits(LOGICAL_TYPE("foo")))));

        // Complex
        final var complexSchema = ColumnarSchema
            .of(STRUCT(LOGICAL_TYPE("outer")).of(STRING, INT, STRUCT.of(DOUBLE, LONG(LOGICAL_TYPE("inner")))));

        testMapSchemaWrapped(complexSchema, new ExtensionArrowColumnDataFactory(new ArrowStructDataFactory(//
            ArrowStringDataFactory.INSTANCE, //
            ArrowIntDataFactory.INSTANCE, //
            new ArrowStructDataFactory(//
                ArrowDoubleDataFactory.INSTANCE, new ExtensionArrowColumnDataFactory(ArrowLongDataFactory.INSTANCE,
                    new DefaultDataTraits(LOGICAL_TYPE("inner"))))),
            complexSchema.getTraits(0)));
    }

    /**
     * Test that wrapping factories with empty traits returns the identical factory,
     * and that even internal traits in nested columns do not change the type of the outer factory
     * **/
    @Test
    public void testWrapEmptyTraits() {
        final var factory = ArrowIntDataFactory.INSTANCE;
        assertEquals(factory, OffHeapArrowSchemaMapper.wrap(factory, DefaultDataTraits.EMPTY));

        final var nestedFactory = new ArrowStructDataFactory(factory);
        final var nestedTraits = ColumnarSchema.of(STRUCT.of(INT(LOGICAL_TYPE("foo")))).getTraits(0);
        assertEquals(nestedFactory, OffHeapArrowSchemaMapper.wrap(nestedFactory, nestedTraits));

        final var listFactory = new ArrowListDataFactory(factory);
        final var listTraits = ColumnarSchema.of(LIST.of(INT(LOGICAL_TYPE("foo")))).getTraits(0);
        assertEquals(listFactory, OffHeapArrowSchemaMapper.wrap(listFactory, listTraits));
    }

    /** Test that ArrowColumnDataFactories are wrapped if the traits contain logical types or dict encoding **/
    @Test
    public void testWrapTraits() {
        final var factory = ArrowIntDataFactory.INSTANCE;
        final var traits = ColumnarSchema.of(INT(LOGICAL_TYPE("foo"))).getTraits(0);
        assertEquals(new ExtensionArrowColumnDataFactory(factory, traits), OffHeapArrowSchemaMapper.wrap(factory, traits));

        final var nestedFactory = new ArrowStructDataFactory(factory);
        final var nestedTraits = ColumnarSchema.of(STRUCT(LOGICAL_TYPE("foo")).of(INT)).getTraits(0);
        assertEquals(new ExtensionArrowColumnDataFactory(new ArrowStructDataFactory(factory), nestedTraits),
            OffHeapArrowSchemaMapper.wrap(nestedFactory, nestedTraits));

        final var listFactory = new ArrowListDataFactory(factory);
        final var listTraits = ColumnarSchema.of(LIST(LOGICAL_TYPE("foo")).of(INT)).getTraits(0);
        assertEquals(new ExtensionArrowColumnDataFactory(new ArrowListDataFactory(factory), listTraits),
            OffHeapArrowSchemaMapper.wrap(listFactory, listTraits));
    }

    /** Test mapping list specs to a {@link ArrowListDataFactory} */
    @Test
    public void testMapListSpec() {
        // Simple
        ListDataSpec spec = new ListDataSpec(DataSpec.doubleSpec());
        testMapSingleSpec(spec, new ArrowListDataFactory(ArrowDoubleDataFactory.INSTANCE));

        // Complex
        StructDataSpec inner = new StructDataSpec(DataSpec.stringSpec(), DataSpec.doubleSpec());
        ListDataSpec middle = new ListDataSpec(inner);
        ListDataSpec outer = new ListDataSpec(middle);
        testMapSingleSpec(outer, new ArrowListDataFactory(//
            new ArrowListDataFactory(//
                new ArrowStructDataFactory(//
                    ArrowStringDataFactory.INSTANCE, ArrowDoubleDataFactory.INSTANCE))));
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
        final OffHeapArrowColumnDataFactory[] factories = OffHeapArrowSchemaMapper.map(schema);
        assertEquals(4, factories.length);
        assertSame(ArrowDoubleDataFactory.INSTANCE, ((ExtensionArrowColumnDataFactory)factories[0]).getDelegate());
        assertSame(ArrowLongDataFactory.INSTANCE, unwrap(factories[1]));
        assertSame(ArrowDoubleDataFactory.INSTANCE, unwrap(factories[2]));
        assertSame(ArrowIntDataFactory.INSTANCE, unwrap(factories[3]));
    }

    /** Test ExtensionArrowColumnDataFactory wraps version of child factory **/
    @Test
    public void testExtensionArrowColumnDataFactoryVersion() {
        final var factory = ArrowIntDataFactory.INSTANCE;
        final var traits = ColumnarSchema.of(INT(LOGICAL_TYPE("foo"))).getTraits(0);
        final var expectedFactory = new ExtensionArrowColumnDataFactory(factory, traits);
        final var wrappedFactory = OffHeapArrowSchemaMapper.wrap(factory, traits);

        assertEquals(expectedFactory.getVersion(), wrappedFactory.getVersion());
        assertEquals(factory.getVersion(), wrappedFactory.getVersion().getChildVersion(0));
    }

    private static OffHeapArrowColumnDataFactory unwrap(final OffHeapArrowColumnDataFactory factory) {
        if (factory instanceof ExtensionArrowColumnDataFactory) {
            return ((ExtensionArrowColumnDataFactory)factory).getDelegate();
        }

        return factory;
    }

    /** Test mapping a single column of the given spec. */
    private static void testMapSingleSpec(final DataSpec spec, final OffHeapArrowColumnDataFactory expectedFactory) {
        var traits = createEmptyTraitsForSpec(spec);
        final ColumnarSchema schema = new DefaultColumnarSchema(spec, traits);
        testMapSchema(schema, expectedFactory);
    }

    private static void testMapSchema(final ColumnarSchema schema, final OffHeapArrowColumnDataFactory expectedFactory) {
        final OffHeapArrowColumnDataFactory[] factories = OffHeapArrowSchemaMapper.map(schema);
        assertEquals(1, factories.length);
        var factory = factories[0];
        assertEquals(expectedFactory, unwrap(factory));
    }

    private static void testMapSchemaWrapped(final ColumnarSchema schema, final OffHeapArrowColumnDataFactory expectedFactory) {
        final OffHeapArrowColumnDataFactory[] factories = OffHeapArrowSchemaMapper.map(schema);
        assertEquals(1, factories.length);
        var factory = factories[0];
        assertEquals(expectedFactory, factory);
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
}
