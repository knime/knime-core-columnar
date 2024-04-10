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
 *   Mar 7, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperWithRowIndexFactory;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;

/**
 * Unit tests for {@link ColumnarVirtualTable}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("static-method")
final class ColumnarVirtualTableTest {

    private static final DataType LONG = LongCell.TYPE;

    private static final DataType INT = IntCell.TYPE;

    private static final DataType DOUBLE = DoubleCell.TYPE;

    private static final DataType STRING = StringCell.TYPE;

    private static final Column FOO = new Column("foo", INT);

    private static final Column BAR = new Column("bar", DOUBLE);

    private static final Column BAZ = new Column("baz", STRING);

    private static final Column BLA = new Column("bla", LONG);

    private static final ColumnarVirtualTable TABLE = createTable(//
        builder(true)//
            .withColumn("foo", INT)//
            .withColumn("bar", DOUBLE)//
            .withColumn("baz", STRING)//
            .build());

    @Test
    void testAppendNoRowIDInAppendedTable() {
        testAppend(true, false);
    }

    @Test
    void testAppendRowIDInAppendedTable() {
        testAppend(true, true);
    }

    @Test
    void testAppendNoRowID() {
        testAppend(false, false);
    }

    @Test
    void testAppendNoRowIDInFirstColumnButRowIDInSecondColumn() {
        testAppend(false, true);
    }

    private static void testAppend(final boolean firstTableHasRowID, final boolean secondTableHasRowID) {
        var firstSchema = builder(firstTableHasRowID)//
            .withColumn("foo", STRING)//
            .withColumn("bar", DOUBLE)//
            .build();

        var secondSchema = builder(secondTableHasRowID)//
            .withColumn("baz", INT)//
            .withColumn("bla", LONG)//
            .build();

        var firstTable = createTable(firstSchema);
        var secondTable = createTable(secondSchema);

        var appendedTable = firstTable.append(List.of(secondTable));

        var expectedSchema = builder(firstTableHasRowID)//
            .withColumn("foo", STRING)//
            .withColumn("bar", DOUBLE)//
            .withColumn("baz", INT)//
            .withColumn("bla", LONG)//
            .build();

        assertEquals(expectedSchema, appendedTable.getSchema());
    }

    @Test
    void testMap() {
        var table = createTable(builder(true).withColumn("foo", INT).build());
        var mappedTable = table.map(new TestMapperFactory(), 1);
        var expectedSchema = builder(false).withColumn("bar", LONG).build();
        assertEquals(expectedSchema, mappedTable.getSchema());
    }

    @Test
    void testPermuteWithRowID() {
        testPermute(true);
        // it's not allowed to move the RowID to a different position
        assertThrows(IllegalArgumentException.class, () -> TABLE.selectColumns(1, 0, 3, 2));
    }

    @Test
    void testPermuteWithoutRowID() {
        testPermute(false);
    }

    private static void testPermute(final boolean withRowID) {
        var table = createTable(withRowID, FOO, BAR, BAZ);
        int[] permutation = withRowID ? new int[] {0, 3, 1, 2} : new int[] {2, 0, 1};
        var permuted = table.selectColumns(permutation);
        var expectedSchema = createSchema(withRowID, BAZ, FOO, BAR);
        assertEquals(expectedSchema, permuted.getSchema());
    }

    @Test
    void testFilterColumnsWithRowID() {
        testFilterColumns(true);
    }

    @Test
    void testFilterColumnsWithoutRowID() {
        testFilterColumns(false);
    }

    private static void testFilterColumns(final boolean withRowID) {
        var table = createTable(withRowID, FOO, BAR, BAZ);
        var filterIndices = withRowID ? new int[] {0, 2} : new int[] {1};
        var filtered = table.selectColumns(filterIndices);
        var expectedSchema = createSchema(withRowID, BAR);
        assertEquals(expectedSchema, filtered.getSchema());
    }

    private static final class TestMapperFactory implements ColumnarMapperWithRowIndexFactory {

        @Override
        public Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var intReadAccess = (IntReadAccess)inputs[0];
            var longWriteAccess = (LongWriteAccess)outputs[0];
            return r -> longWriteAccess.setLongValue(intReadAccess.getIntValue() + r);
        }

        @Override
        public ValueSchema getOutputSchema() {
            return builder(false).withColumn("bar", LONG).build();
        }

    }

    private static ColumnarVirtualTable createTable(final ValueSchema schema) {
        return new ColumnarVirtualTable(UUID.randomUUID(), schema, true);
    }

    private static ColumnarVirtualTable createTable(final boolean hasRowID, final Column... columns) {
        return createTable(createSchema(hasRowID, columns));
    }

    private static ValueSchema createSchema(final boolean hasRowID, final Column... columns) {
        var schemaBuilder = builder(hasRowID);
        Stream.of(columns).forEach(schemaBuilder::withColumn);
        return schemaBuilder.build();
    }

    private static ColumnarValueSchemaBuilder builder(final boolean hasRowID) {
        return new ColumnarValueSchemaBuilder(hasRowID);
    }

    record Column(String name, DataType type) {
    }

    private static final class ColumnarValueSchemaBuilder {

        private final boolean m_hasRowID;

        private final List<DataColumnSpec> m_columns = new ArrayList<>();

        private ColumnarValueSchemaBuilder(final boolean hasRowID) {
            m_hasRowID = hasRowID;
        }

        ColumnarValueSchemaBuilder withColumn(final String name, final DataType type) {
            m_columns.add(new DataColumnSpecCreator(name, type).createSpec());
            return this;
        }

        ColumnarValueSchemaBuilder withColumn(final Column column) {
            return withColumn(column.name, column.type);
        }

        ValueSchema build() {
            var valueFactories = Stream
                .concat(m_hasRowID ? Stream.of(DefaultRowKeyValueFactory.INSTANCE) : Stream.empty(),
                    // null is save here because we are not using any FileStoreAwareValueFactories
                    m_columns.stream().map(c -> ValueFactoryUtils.getValueFactory(c.getType(), null)))//
                .toArray(ValueFactory<?, ?>[]::new);
            var spec = new DataTableSpec(m_columns.toArray(DataColumnSpec[]::new));
            return ValueSchemaUtils.create(spec, valueFactories);
        }
    }
}
