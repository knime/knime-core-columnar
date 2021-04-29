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

import org.junit.Test;
import org.knime.core.columnar.arrow.data.ArrowBooleanData.ArrowBooleanDataFactory;
import org.knime.core.columnar.arrow.data.ArrowByteData.ArrowByteDataFactory;
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
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;

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

    /** Test mapping String specs to a {@link ArrowStringDataFactory} */
    @Test
    public void testMapStringSpec() {
        testMapSingleSpec(DataSpec.stringSpec(), ArrowStringDataFactory.INSTANCE);
    }

    /** Test mapping void specs to a {@link ArrowStructDataFactory} */
    @Test
    public void testMapStructSpec() {
        // Simple
        testMapSingleSpec(new StructDataSpec(DataSpec.doubleSpec(), DataSpec.intSpec()),
            new ArrowStructDataFactory(ArrowDoubleDataFactory.INSTANCE, ArrowIntDataFactory.INSTANCE));

        // Complex
        testMapSingleSpec(
            new StructDataSpec(DataSpec.stringSpec(), DataSpec.intSpec(),
                new StructDataSpec(DataSpec.doubleSpec(), DataSpec.longSpec())),
            new ArrowStructDataFactory(ArrowStringDataFactory.INSTANCE, ArrowIntDataFactory.INSTANCE,
                new ArrowStructDataFactory(ArrowDoubleDataFactory.INSTANCE, ArrowLongDataFactory.INSTANCE)));
    }

    /** Test mapping list specs to a {@link ArrowListDataFactory} */
    @Test
    public void testMapListSpec() {
        // Simple
        testMapSingleSpec(new ListDataSpec(DataSpec.doubleSpec()),
            new ArrowListDataFactory(ArrowDoubleDataFactory.INSTANCE));

        // Complex
        testMapSingleSpec(
            new ListDataSpec(new ListDataSpec(new StructDataSpec(DataSpec.stringSpec(), DataSpec.doubleSpec()))),
            new ArrowListDataFactory(new ArrowListDataFactory(
                new ArrowStructDataFactory(ArrowStringDataFactory.INSTANCE, ArrowDoubleDataFactory.INSTANCE))));
    }

    // TODO test for other specs when implemented

    /** Test mapping multiple columns of different specs. */
    @Test
    public void testMappingMultipleColumns() {
        final ColumnarSchema schema = ArrowTestUtils.createSchema( //
            DataSpec.doubleSpec(), //
            DataSpec.longSpec(), //
            DataSpec.doubleSpec(), //
            DataSpec.intSpec() //
        );
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(schema);
        assertEquals(4, factories.length);
        assertSame(ArrowDoubleDataFactory.INSTANCE, factories[0]);
        assertSame(ArrowLongDataFactory.INSTANCE, factories[1]);
        assertSame(ArrowDoubleDataFactory.INSTANCE, factories[2]);
        assertSame(ArrowIntDataFactory.INSTANCE, factories[3]);
    }

    /** Test mapping a single column of the given spec. */
    private static void testMapSingleSpec(final DataSpec spec, final ArrowColumnDataFactory expectedFactory) {
        final ColumnarSchema schema = ArrowTestUtils.createSchema(spec);
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(schema);
        assertEquals(1, factories.length);
        assertEquals(expectedFactory, factories[0]);
    }
}
