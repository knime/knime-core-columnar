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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.knime.core.data.DataType;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperWithRowIndexFactory;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchema.ValueSchemaColumn;
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

    private static final ValueSchemaColumn ROWID = new ValueSchemaColumn(DefaultRowKeyValueFactory.INSTANCE);

    private static final ValueSchemaColumn FOO = column("foo", INT);

    private static final ValueSchemaColumn BAR = column("bar", DOUBLE);

    private static final ValueSchemaColumn BAZ = column("baz", STRING);

    @Test
    void testAppend() {
        var firstSchema = builder()//
            .withRowID()//
            .withColumn("foo", STRING)//
            .withColumn("bar", DOUBLE)//
            .build();

        var secondSchema = builder()//
            .withColumn("baz", INT)//
            .withColumn("bla", LONG)//
            .build();

        var firstTable = createTable(firstSchema);
        var secondTable = createTable(secondSchema);

        var appendedTable = firstTable.append(secondTable);

        var expectedSchema = builder()//
            .withRowID()//
            .withColumn("foo", STRING)//
            .withColumn("bar", DOUBLE)//
            .withColumn("baz", INT)//
            .withColumn("bla", LONG)//
            .build();

        assertEquals(expectedSchema, appendedTable.getSchema());
    }

    @Test
    void testAppendWithRowID() {
        var firstSchema = builder()//
            .withRowID()//
            .withColumn("foo", STRING)//
            .build();

        var secondSchema = builder()//
            .withRowID()//
            .withColumn("bar", DOUBLE)//
            .build();

        var firstTable = createTable(firstSchema);
        var secondTable = createTable(secondSchema);

        var appendedTable = firstTable.append(secondTable);

        var expectedSchema = builder()//
            .withRowID()//
            .withColumn("foo", STRING)//
            .withRowID()//
            .withColumn("bar", DOUBLE)//
            .build();

        assertEquals(expectedSchema, appendedTable.getSchema());
    }

    @Test
    void testMap() {
        var table = createTable(builder().withRowID().withColumn("foo", INT).build());
        var mappedTable = table.map(new TestMapperFactory(), 1);
        var expectedSchema = builder().withColumn("bar", LONG).build();
        assertEquals(expectedSchema, mappedTable.getSchema());
    }

    @Test
    void testPermuteWithRowID() {
        var table = createTable(ROWID, FOO, BAR, BAZ);
        int[] permutation = new int[]{1, 0, 3, 2};
        var permuted = table.selectColumns(permutation);
        var expectedSchema = createSchema(FOO, ROWID, BAZ, BAR);
        assertEquals(expectedSchema, permuted.getSchema());
    }

    @Test
    void testPermuteWithoutRowID() {
        var table = createTable(FOO, BAR, BAZ);
        int[] permutation = new int[]{2, 0, 1};
        var permuted = table.selectColumns(permutation);
        var expectedSchema = createSchema(BAZ, FOO, BAR);
        assertEquals(expectedSchema, permuted.getSchema());
    }

    @Test
    void testFilterColumnsWithRowID() {
        var table = createTable(ROWID, FOO, BAR, BAZ);
        var filterIndices = new int[]{0, 2};
        var filtered = table.selectColumns(filterIndices);
        var expectedSchema = createSchema(ROWID, BAR);
        assertEquals(expectedSchema, filtered.getSchema());
    }

    @Test
    void testFilterColumnsWithoutRowID() {
        var table = createTable(FOO, BAR, BAZ);
        var filterIndices = new int[]{1};
        var filtered = table.selectColumns(filterIndices);
        var expectedSchema = createSchema(BAR);
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
            return builder().withColumn("bar", LONG).build();
        }

    }

    private static ColumnarVirtualTable createTable(final ValueSchema schema) {
        return new ColumnarVirtualTable(UUID.randomUUID(), schema, true);
    }

    private static ColumnarVirtualTable createTable(final ValueSchemaColumn... columns) {
        return createTable(createSchema(columns));
    }

    private static ValueSchema createSchema(final ValueSchemaColumn... columns) {
        var schemaBuilder = builder();
        Stream.of(columns).forEach(schemaBuilder::withColumn);
        return schemaBuilder.build();
    }

    private static ValueSchemaColumn column(final String name, final DataType type) {
        return new ValueSchemaColumn(name, type, ValueFactoryUtils.getValueFactory(type, null));
    }

    private static ValueSchemaBuilder builder() {
        return new ValueSchemaBuilder();
    }

    private static final class ValueSchemaBuilder {

        private final List<ValueSchemaColumn> m_columns = new ArrayList<>();

        ValueSchemaBuilder withRowID() {
            m_columns.add(new ValueSchemaColumn(DefaultRowKeyValueFactory.INSTANCE));
            return this;
        }

        ValueSchemaBuilder withColumn(final String name, final DataType type) {
            m_columns.add(column(name, type));
            return this;
        }

        ValueSchemaBuilder withColumn(final ValueSchemaColumn column) {
            m_columns.add(column);
            return this;
        }

        ValueSchema build() {
            return ValueSchemaUtils.create(m_columns);
        }
    }
}
