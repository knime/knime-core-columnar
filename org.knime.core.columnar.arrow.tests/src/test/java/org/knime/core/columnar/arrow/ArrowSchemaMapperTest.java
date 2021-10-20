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
import static org.junit.Assert.assertTrue;
import static org.knime.core.table.schema.DataSpecs.DICT_ENCODING;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.schema.DataSpecs.VARBINARY;

import java.util.Arrays;

import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowSchemaMapper.ExtensionArrowColumnDataFactory;
import org.knime.core.columnar.arrow.data.ArrowBooleanData.ArrowBooleanDataFactory;
import org.knime.core.columnar.arrow.data.ArrowByteData.ArrowByteDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedStringData.ArrowDictEncodedStringDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedVarBinaryData.ArrowDictEncodedVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDurationData.ArrowDurationDataFactory;
import org.knime.core.columnar.arrow.data.ArrowFloatData.ArrowFloatDataFactory;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.data.ArrowListData.ArrowListDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLocalDateData.ArrowLocalDateDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLocalDateTimeData.ArrowLocalDateTimeDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLocalTimeData.ArrowLocalTimeDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLongData.ArrowLongDataFactory;
import org.knime.core.columnar.arrow.data.ArrowPeriodData.ArrowPeriodDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStringData.ArrowStringDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.data.ArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.ArrowVoidData.ArrowVoidDataFactory;
import org.knime.core.columnar.arrow.data.ArrowZonedDateTimeData.ArrowZonedDateTimeDataFactory;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DataTraitUtils;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DefaultListDataTraits;
import org.knime.core.table.schema.traits.DefaultStructDataTraits;

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

    /** Test mapping LocalTime specs to a {@link ArrowLocalTimeDataFactory} */
    @Test
    public void testMapLocalTimeSpec() {
        testMapSingleSpec(DataSpec.localTimeSpec(), ArrowLocalTimeDataFactory.INSTANCE);
    }

    /** Test mapping LocalDate specs to a {@link ArrowLocalDateDataFactory} */
    @Test
    public void testMapLocalDateSpec() {
        testMapSingleSpec(DataSpec.localDateSpec(), ArrowLocalDateDataFactory.INSTANCE);
    }

    /** Test mapping Period specs to a {@link ArrowPeriodDataFactory} */
    @Test
    public void testMapPeriodSpec() {
        testMapSingleSpec(DataSpec.periodSpec(), ArrowPeriodDataFactory.INSTANCE);
    }

    /** Test mapping Duration specs to a {@link ArrowDurationDataFactory} */
    @Test
    public void testMapDurationSpec() {
        testMapSingleSpec(DataSpec.durationSpec(), ArrowDurationDataFactory.INSTANCE);
    }

    /** Test mapping LocalDateTime specs to a {@link ArrowLocalDateTimeDataFactory} */
    @Test
    public void testMapLocalDateTimeSpec() {
        testMapSingleSpec(DataSpec.localDateTimeSpec(), ArrowLocalDateTimeDataFactory.INSTANCE);
    }

    /** Test mapping ZonedDateTime specs to a {@link ArrowZonedDateTimeDataFactory} */
    @Test
    public void testMapZonedDateTimeSpec() {
        testMapSingleSpec(DataSpec.zonedDateTimeSpec(), ArrowZonedDateTimeDataFactory.INSTANCE);
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
        testMapSingleSpec(spec, new ArrowStructDataFactory(wrapSimple(ArrowDoubleDataFactory.INSTANCE),
            wrapSimple(ArrowIntDataFactory.INSTANCE)));

        // Complex
        StructDataSpec inner = new StructDataSpec(DataSpec.doubleSpec(), DataSpec.longSpec());
        StructDataSpec outer = new StructDataSpec(DataSpec.stringSpec(), DataSpec.intSpec(), inner);
        testMapSingleSpec(outer,
            new ArrowStructDataFactory(wrapSimple(ArrowStringDataFactory.INSTANCE),
                wrapSimple(ArrowIntDataFactory.INSTANCE),
                wrapComplex(new ArrowStructDataFactory(wrapSimple(ArrowDoubleDataFactory.INSTANCE),
                    wrapSimple(ArrowLongDataFactory.INSTANCE)), inner)));
    }

    private static ArrowColumnDataFactory wrapSimple(final ArrowColumnDataFactory factory) {
        return ArrowSchemaMapper.wrap(factory, DefaultDataTraits.EMPTY);
    }

    private static ArrowColumnDataFactory wrapComplex(final ArrowColumnDataFactory factory, final DataSpec spec) {
        return ArrowSchemaMapper.wrap(factory, DataTraitUtils.emptyTraits(spec));
    }

    /** Test mapping list specs to a {@link ArrowListDataFactory} */
    @Test
    public void testMapListSpec() {
        // Simple
        ListDataSpec spec = new ListDataSpec(DataSpec.doubleSpec());
        testMapSingleSpec(spec,
            new ArrowListDataFactory(wrapSimple(ArrowDoubleDataFactory.INSTANCE)));

        // Complex
        StructDataSpec inner = new StructDataSpec(DataSpec.stringSpec(), DataSpec.doubleSpec());
        ListDataSpec middle = new ListDataSpec(inner);
        ListDataSpec outer = new ListDataSpec(middle);
        testMapSingleSpec(outer,
            new ArrowListDataFactory(wrapComplex(new ArrowListDataFactory(
                wrapComplex(new ArrowStructDataFactory(wrapSimple(ArrowStringDataFactory.INSTANCE),
                    wrapSimple(ArrowDoubleDataFactory.INSTANCE)), inner)),
                middle)));
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
            DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY, DefaultDataTraits.EMPTY};

        final ColumnarSchema schema = new DefaultColumnarSchema(specs, traits);
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(schema);
        assertEquals(4, factories.length);
        assertSame(ArrowDoubleDataFactory.INSTANCE, ((ExtensionArrowColumnDataFactory)factories[0]).getDelegate());
        assertSame(ArrowLongDataFactory.INSTANCE, unwrap(factories[1]));
        assertSame(ArrowDoubleDataFactory.INSTANCE, unwrap(factories[2]));
        assertSame(ArrowIntDataFactory.INSTANCE, unwrap(factories[3]));
    }

    private static ArrowColumnDataFactory unwrap(final ArrowColumnDataFactory factory) {
        assertTrue(factory instanceof ExtensionArrowColumnDataFactory);
        return ((ExtensionArrowColumnDataFactory)factory).getDelegate();
    }

    /** Test mapping a single column of the given spec. */
    private static void testMapSingleSpec(final DataSpec spec, final ArrowColumnDataFactory expectedFactory) {
        var traits = createTraitsForSpec(spec);
        final ColumnarSchema schema = new DefaultColumnarSchema(spec, traits);
        testMapSchema(schema, expectedFactory);
    }

    private static void testMapSchema(final ColumnarSchema schema, final ArrowColumnDataFactory expectedFactory) {
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(schema);
        assertEquals(1, factories.length);
        var factory = factories[0];
        assertEquals(expectedFactory, unwrap(factory));
    }

    private static DataTraits createTraitsForSpec(final DataSpec spec) {
        if (spec instanceof StructDataSpec) {
            var structSpec = (StructDataSpec)spec;
            final DataTraits[] dataTraits = new DataTraits[structSpec.size()];
            Arrays.setAll(dataTraits, i -> createTraitsForSpec(structSpec.getDataSpec(i)));
            return new DefaultStructDataTraits(dataTraits);
        } else if (spec instanceof ListDataSpec) {
            var innerTraits = createTraitsForSpec(((ListDataSpec)spec).getInner());
            return new DefaultListDataTraits(innerTraits);
        }
        return DefaultDataTraits.EMPTY;
    }

    private static DataTraits createDictEncodingTraits(final KeyType keyType) {
        return new DefaultDataTraits(new DataTrait.DictEncodingTrait(keyType));
    }
}
