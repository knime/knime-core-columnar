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
import org.knime.core.columnar.arrow.AbstractArrowDataTest.DummyByteArraySerializer;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedObjectData.ArrowDictEncodedObjectDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDoubleData.ArrowDoubleDataFactory;
import org.knime.core.columnar.arrow.data.ArrowFloatData.ArrowFloatDataFactory;
import org.knime.core.columnar.arrow.data.ArrowIntData.ArrowIntDataFactory;
import org.knime.core.columnar.arrow.data.ArrowLongData.ArrowLongDataFactory;
import org.knime.core.columnar.arrow.data.ArrowObjectData.ArrowObjectDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.data.ArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.arrow.data.ArrowVoidData.ArrowVoidDataFactory;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.DoubleData.DoubleDataSpec;
import org.knime.core.columnar.data.FloatData.FloatDataSpec;
import org.knime.core.columnar.data.IntData.IntDataSpec;
import org.knime.core.columnar.data.LongData.LongDataSpec;
import org.knime.core.columnar.data.ObjectData.ObjectDataSpec;
import org.knime.core.columnar.data.StructData.StructDataSpec;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryDataSpec;
import org.knime.core.columnar.data.VoidData.VoidDataSpec;
import org.knime.core.columnar.store.ColumnStoreSchema;

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
        testMapSingleSpec(DoubleDataSpec.INSTANCE, ArrowDoubleDataFactory.INSTANCE);
    }

    /** Test mapping int specs to a {@link ArrowIntDataFactory} */
    @Test
    public void testMapIntSpec() {
        testMapSingleSpec(IntDataSpec.INSTANCE, ArrowIntDataFactory.INSTANCE);
    }

    /** Test mapping long specs to a {@link ArrowLongDataFactory} */
    @Test
    public void testMapLongSpec() {
        testMapSingleSpec(LongDataSpec.INSTANCE, ArrowLongDataFactory.INSTANCE);
    }

    /** Test mapping float specs to a {@link ArrowFloatDataFactory} */
    @Test
    public void testMapFloatSpec() {
        testMapSingleSpec(FloatDataSpec.INSTANCE, ArrowFloatDataFactory.INSTANCE);
    }

    /** Test mapping var binary specs to a {@link ArrowVarBinaryDataFactory} */
    @Test
    public void testMapVarBinarySpec() {
        testMapSingleSpec(VarBinaryDataSpec.INSTANCE, ArrowVarBinaryDataFactory.INSTANCE);
    }

    /** Test mapping object specs to a {@link ArrowObjectDataFactory} */
    @Test
    public void testMapObjectSpec() {
        testMapSingleSpec(new ObjectDataSpec<>(DummyByteArraySerializer.INSTANCE, false),
            new ArrowObjectDataFactory<>(DummyByteArraySerializer.INSTANCE));
    }

    /** Test mapping dict encoded byte specs to a {@link ArrowDictEncodedObjectDataFactory} */
    @Test
    public void testMapDictEncodedSpec() {
        testMapSingleSpec(new ObjectDataSpec<>(DummyByteArraySerializer.INSTANCE, true),
            new ArrowDictEncodedObjectDataFactory<>(DummyByteArraySerializer.INSTANCE));
    }

    /** Test mapping void specs to a {@link ArrowVoidDataFactory} */
    @Test
    public void testMapVoidSpec() {
        testMapSingleSpec(VoidDataSpec.INSTANCE, ArrowVoidDataFactory.INSTANCE);
    }

    /** Test mapping void specs to a {@link ArrowStructDataFactory} */
    @Test
    public void testMapStructSpec() {
        // Simple
        testMapSingleSpec(new StructDataSpec(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE),
            new ArrowStructDataFactory(ArrowDoubleDataFactory.INSTANCE, ArrowIntDataFactory.INSTANCE));

        // Complex
        testMapSingleSpec(
            new StructDataSpec(new ObjectDataSpec<>(DummyByteArraySerializer.INSTANCE, true), IntDataSpec.INSTANCE,
                new StructDataSpec(new ObjectDataSpec<>(DummyByteArraySerializer.INSTANCE, false),
                    new ObjectDataSpec<>(DummyByteArraySerializer.INSTANCE, true))),
            new ArrowStructDataFactory(new ArrowDictEncodedObjectDataFactory<>(DummyByteArraySerializer.INSTANCE),
                ArrowIntDataFactory.INSTANCE,
                new ArrowStructDataFactory(new ArrowObjectDataFactory<>(DummyByteArraySerializer.INSTANCE),
                    new ArrowDictEncodedObjectDataFactory<>(DummyByteArraySerializer.INSTANCE))));
    }

    // TODO test for other specs when implemented

    /** Test mapping multiple columns of different specs. */
    @Test
    public void testMappingMultipleColumns() {
        final ColumnStoreSchema schema = ArrowTestUtils.createSchema( //
            DoubleDataSpec.INSTANCE, //
            LongDataSpec.INSTANCE, //
            DoubleDataSpec.INSTANCE, //
            IntDataSpec.INSTANCE //
        );
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(schema);
        assertEquals(4, factories.length);
        assertSame(ArrowDoubleDataFactory.INSTANCE, factories[0]);
        assertSame(ArrowLongDataFactory.INSTANCE, factories[1]);
        assertSame(ArrowDoubleDataFactory.INSTANCE, factories[2]);
        assertSame(ArrowIntDataFactory.INSTANCE, factories[3]);
    }

    /** Test mapping a single column of the given spec. */
    private static void testMapSingleSpec(final ColumnDataSpec spec, final ArrowColumnDataFactory expectedFactory) {
        final ColumnStoreSchema schema = ArrowTestUtils.createSchema(spec);
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(schema);
        assertEquals(1, factories.length);
        assertEquals(expectedFactory, factories[0]);
    }
}
