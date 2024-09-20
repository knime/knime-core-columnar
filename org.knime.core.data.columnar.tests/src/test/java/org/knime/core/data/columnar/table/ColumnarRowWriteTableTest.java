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
 *   Oct 30, 2021 (marcel): created
 */
package org.knime.core.data.columnar.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IntValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.StringValue;
import org.knime.core.data.columnar.table.ColumnarTableTestUtils.TestColumnStoreFactory;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.IntCell.IntCellFactory;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.def.StringCell.StringCellFactory;
import org.knime.core.data.v2.RowBuffer;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.data.v2.value.ValueInterfaces.IntWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringWriteValue;

/**
 * Only tests functionality not yet covered (indirectly) by {@link ColumnarRowContainerTest} or
 * {@link ColumnarRowWriteCursorTest}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class ColumnarRowWriteTableTest {

    @Test
    public void testDomainInitializationNoDomainComputation() throws IOException {
        final ColumnarRowWriteTableSettings settings = createSettings(true, false);

        final ValueSchema initialSchema = createTestTableSchemaWithDomains();
        final DataTableSpec resultingSpec = createAndFillTestTable(initialSchema, settings);

        assertEquals(initialSchema.getSourceSpec(), resultingSpec);
    }

    @Test
    public void testNoDomainInitializationDomainComputation() throws IOException {
        final ColumnarRowWriteTableSettings settings = createSettings(false, true);

        final ValueSchema initialSchema = createTestTableSchemaWithDomains();
        final DataTableSpec resultingSpec = createAndFillTestTable(initialSchema, settings);

        assertTrue(initialSchema.getSourceSpec().equalStructure(resultingSpec));
        checkDomains(-234, 21, new String[]{"My third string", "My fourth string"}, resultingSpec);
    }

    @Test
    public void testDomainInitializationDomainComputation() throws IOException {
        final ColumnarRowWriteTableSettings settings = createSettings(true, true);

        final ValueSchema initialSchema = createTestTableSchemaWithDomains();
        final DataTableSpec resultingSpec = createAndFillTestTable(initialSchema, settings);

        assertTrue(initialSchema.getSourceSpec().equalStructure(resultingSpec));
        checkDomains(-234, 42,
            new String[]{"My first string", "My second string", "My third string", "My fourth string"}, resultingSpec);
    }

    @Test(expected = IllegalStateException.class)
    public void testRejectsSetMaxPossibleValuesWhenNotComputingDomains() throws IOException {
        final var settings = createSettings(true, false);

        try (final ColumnarRowWriteTable table = createTestTable(createTestTableSchemaWithDomains(), settings)) {
            table.setMaxPossibleValues(50);
        }
    }

    private static ColumnarRowWriteTableSettings createSettings(final boolean initializeDomains,
        final boolean calculateDomains) {
        return new ColumnarRowWriteTableSettings(initializeDomains, calculateDomains, 60, true, true, false, 100, 4);
    }

    private static ValueSchema createTestTableSchemaWithDomains() {
        final var intColumnSpecCreator = new DataColumnSpecCreator("My int column", IntCell.TYPE);
        intColumnSpecCreator.setDomain(
            new DataColumnDomainCreator(IntCellFactory.create(-123), IntCellFactory.create(42)).createDomain());
        final DataColumnSpec intColumnSpec = intColumnSpecCreator.createSpec();

        final var stringColumnSpecCreator = new DataColumnSpecCreator("My string column", StringCell.TYPE);
        stringColumnSpecCreator.setDomain(new DataColumnDomainCreator(
            new DataCell[]{StringCellFactory.create("My first string"), StringCellFactory.create("My second string")})
                .createDomain());
        final DataColumnSpec stringColumnSpec = stringColumnSpecCreator.createSpec();

        final var tableSpec = new DataTableSpec(intColumnSpec, stringColumnSpec);
        return ValueSchemaUtils.create(tableSpec, RowKeyType.CUSTOM, null);
    }

    private static DataTableSpec createAndFillTestTable(final ValueSchema schema,
        final ColumnarRowWriteTableSettings settings) throws IOException {
        final ColumnarRowReadTable readTable;
        try (final var writeTable = createTestTable(schema, settings)) {
            try (final var cursor = writeTable.createCursor()) {
                writeTestData(cursor, schema);
                readTable = writeTable.finish();
            }
        }
        try (readTable) {
            return readTable.getSchema().getSourceSpec();
        }
    }

    private static ColumnarRowWriteTable createTestTable(final ValueSchema schema,
        final ColumnarRowWriteTableSettings settings) throws IOException {
        return new ColumnarRowWriteTable(schema, TestColumnStoreFactory.INSTANCE, settings);
    }

    private static void writeTestData(final ColumnarRowWriteCursor cursor, final ValueSchema schema) {
        final RowBuffer row = RowBuffer.forSchema(schema);

        row.setRowKey(RowKey.createRowKey(0l));
        row.<IntWriteValue> getWriteValue(0).setIntValue(-234);
        row.<StringWriteValue> getWriteValue(1).setStringValue("My third string");
        cursor.commit(row);

        row.setRowKey(RowKey.createRowKey(1l));
        row.<IntWriteValue> getWriteValue(0).setIntValue(21);
        row.<StringWriteValue> getWriteValue(1).setStringValue("My fourth string");
        cursor.commit(row);
    }

    private static void checkDomains(final int intColExpectedLowerBound, final int intColExpectedUpperBound,
        final String[] stringColExpectedValues, final DataTableSpec actualTableSpec) {
        final DataColumnDomain intColActualDomain = actualTableSpec.getColumnSpec(0).getDomain();
        assertEquals(intColExpectedLowerBound, ((IntValue)intColActualDomain.getLowerBound()).getIntValue());
        assertEquals(intColExpectedUpperBound, ((IntValue)intColActualDomain.getUpperBound()).getIntValue());

        final DataColumnDomain stringColActualDomain = actualTableSpec.getColumnSpec(1).getDomain();
        final Set<String> stringColActualValues = stringColActualDomain.getValues().stream() //
            .map(c -> ((StringValue)c).getStringValue()) //
            .collect(Collectors.toSet());
        assertEquals(Set.of(stringColExpectedValues), stringColActualValues);
    }
}
