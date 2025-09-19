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
 *   Oct 8, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.append.AppendedRowsTable;
import org.knime.core.data.container.ConcatenateTable;
import org.knime.core.data.container.JoinedTable;
import org.knime.core.data.container.RearrangeColumnsTable;
import org.knime.core.data.container.TableSpecReplacerTable;
import org.knime.core.data.container.WrappedTable;
import org.knime.core.data.v2.RowKeyValueFactory;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.DataTableValueSchema;
import org.knime.core.data.v2.schema.DataTableValueSchemaUtils;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.Node;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.virtual.ColumnarSchemas;

/**
 * Provides methods for extraction and creation of DataTableValueSchemas from all known KnowsRowCountTables that might
 * be encountered with the Columnar Table Backend.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @noreference Not intended to be referenced by clients
 */
public final class VirtualTableSchemaUtils {

    private static final DataTableValueSchema EMPTY =
        DataTableValueSchemaUtils.create(new DataTableSpec(), new DefaultRowKeyValueFactory());

    private static RowKeyValueFactory<?, ?> getRowKeyValueFactory(final ValueSchema[] schemas)
        throws VirtualTableIncompatibleException {
        RowKeyValueFactory<?, ?> commonFactory = (RowKeyValueFactory<?, ?>)schemas[0].getValueFactory(0);
        for (int i = 1; i < schemas.length; i++) {//NOSONAR
            var factory = schemas[i].getValueFactory(0);
            if (!ValueFactoryUtils.areEqual(commonFactory, factory)) {
                throw new VirtualTableIncompatibleException("Conflicting RowKeyValueFactories detected.");
            }
        }
        return commonFactory;
    }

    private static void checkFactoriesMatch(final String name, final DataTableValueSchema schema,
        final Map<String, ValueFactory<?, ?>> factoriesByName) throws VirtualTableIncompatibleException {
        final var factory = getValueFactoryForName(schema, name);
        if (factory != null) {
            final var commonFactory = factoriesByName.computeIfAbsent(name, n -> factory);
            if (!ValueFactoryUtils.areEqual(factory, commonFactory)) {
                throw new VirtualTableIncompatibleException(
                    "Incompatible ValueFactories '%s' and '%s' for the column '%s'.", factory, commonFactory, name);
            }
        }
    }

    private static ValueFactory<?, ?> getValueFactoryForName(final DataTableValueSchema schema, final String name) {
        final int colIdx = schema.getSourceSpec().findColumnIndex(name);
        if (colIdx > -1) {
            // +1 because of the RowKeyValueFactory
            return schema.getValueFactory(colIdx + 1);
        } else {
            return null;
        }
    }

    /**
     * Appends the DataTableValueSchemas of the provided BufferedDataTables.
     *
     * @param tables to append
     * @return the appended DataTableValueSchema
     * @throws VirtualTableIncompatibleException if any of the involved tables (including reference tables) are not
     *             compatible with virtual tables (created prior to 4.5.0 or concatenation with varying ValueFactories
     *             in any column)
     */
    public static DataTableValueSchema appendSchemas(final BufferedDataTable[] tables)
        throws VirtualTableIncompatibleException {
        DataTableSpec tableSpec = null;
        final List<ValueFactory<?, ?>> factories = new ArrayList<>();
        var isFirst = true;
        for (var table : tables) {//NOSONAR
            final var schema = extractSchema(table);
            if (isFirst) {
                factories.add(schema.getValueFactory(0));
                tableSpec = table.getDataTableSpec();
                isFirst = false;
            } else {
                tableSpec = new DataTableSpec(tableSpec, table.getDataTableSpec());
            }
            IntStream.range(1, schema.numColumns()).forEach(c -> factories.add(schema.getValueFactory(c)));
        }
        return createSchema(tableSpec, factories);
    }

    /**
     * Appends the {@link DataTableSpec specs} of the given {@code tables}.
     *
     * @param tables tables whose DataTableSpecs should be concatenated
     * @return the concatenated {@code DataTableSpec}
     */
    public static DataTableSpec appendDataTableSpecs(final BufferedDataTable[] tables) {
        DataTableSpec appended = null;
        for (var table : tables) {//NOSONAR
            final var spec = table.getDataTableSpec();
            appended = (appended == null) ? spec : new DataTableSpec(appended, spec);
        }
        return appended;
    }

    /**
     * {@link DataTableValueSchema#getSourceSpec() Extract} the {@code DataTableSpec}s of the given {@code schemas}.
     *
     * @param schemas to extract the DataTableSpecs from
     * @return the extracted DataTableSpecs
     */
    public static DataTableSpec[] extractSpecs(final DataTableValueSchema[] schemas) {
        return Stream.of(schemas)//
            .map(DataTableValueSchema::getSourceSpec)//
            .toArray(DataTableSpec[]::new);
    }

    /**
     * Extract the {@code DataTableValueSchema}s from the given {@code tables}.
     *
     * @param tables to extract the DataTableValueSchemas from
     * @return the extracted schemas
     * @throws VirtualTableIncompatibleException if any of the tables is not compatible with columnar virtual tables
     */
    public static DataTableValueSchema[] extractSchemas(final BufferedDataTable[] tables)
        throws VirtualTableIncompatibleException {
        var schemas = new DataTableValueSchema[tables.length];
        for (int i = 0; i < schemas.length; i++) {//NOSONAR
            schemas[i] = extractSchema(tables[i]);
        }
        return schemas;
    }

    /**
     * Extract a {@code DataTableValueSchema} from the given {@code table}.
     *
     * @param table to extract the DataTableValueSchema from
     * @return the extracted schema
     * @throws VirtualTableIncompatibleException if the table is not compatible with columnar virtual tables
     */
    public static DataTableValueSchema extractSchema(final BufferedDataTable table)
        throws VirtualTableIncompatibleException {
        var delegateTable = Node.invokeGetDelegate(table);
        var schema = extractSchema(delegateTable);
        if (DataTableValueSchemaUtils.storesDataCellSerializersSeparately(schema)) {
            throw new VirtualTableIncompatibleException(
                "Tables created before KNIME Analytics Platform 4.5.0 are not compatible with virtual tables.");
        }
        return schema;
    }

    private static DataTableValueSchema extractSchema(final KnowsRowCountTable table)//NOSONAR
        throws VirtualTableIncompatibleException {
        if (table instanceof AbstractColumnarContainerTable t) {
            return t.getSchema();
        } else if (table instanceof VirtualTableExtensionTable t) {
            return t.getSchema();
        } else if (table instanceof RearrangeColumnsTable t) {
            return extractSchemaFromRearrangeTable(t);
        } else if (table instanceof JoinedTable t) {
            return extractSchemaFromJoinedTable(t);
        } else if (table instanceof TableSpecReplacerTable t) {
            return extractSchemaFromSpecReplacerTable(t);
        } else if (table instanceof WrappedTable t) {
            return extractSchemaFromWrappedTable(t);
        } else if (table instanceof ConcatenateTable t) {
            return extractSchemaFromConcatenateTable(t);
        } else {
            throw new VirtualTableIncompatibleException(
                String.format("The table type '%s' is not supported.", table.getClass()));
        }
    }


    /**
     * Extract and concatenate the DataTableValueSchemas of the concatenated tables.
     */
    private static DataTableValueSchema extractSchemaFromConcatenateTable(final ConcatenateTable table)
        throws VirtualTableIncompatibleException {

        final BufferedDataTable[] tables = table.getReferenceTables();

        final var schemas = extractSchemas(tables);
        try {
            ColumnarSchemas.concatenate(List.of(schemas));
        } catch (IllegalArgumentException ex) {
            throw new VirtualTableIncompatibleException("Schemas don't match exactly.", ex);
        }

        final DataTableSpec[] tableSpecs = extractSpecs(schemas);
        final DataTableSpec outputSpec = AppendedRowsTable.generateDataTableSpec(tableSpecs);
        final Map<String, ValueFactory<?, ?>> factoriesByName = new LinkedHashMap<>(outputSpec.getNumColumns());
        factoriesByName.put("", getRowKeyValueFactory(schemas));
        // using "" is safe because no column can have an empty name in KNIME
        for (var column : outputSpec) {
            final var name = column.getName();
            for (int i = 0; i < schemas.length; i++) {//NOSONAR
                checkFactoriesMatch(name, schemas[i], factoriesByName);
            }
        }
        return createSchema(outputSpec, factoriesByName.values());
    }


    @SuppressWarnings("resource")
    private static DataTableValueSchema extractSchemaFromRearrangeTable(final RearrangeColumnsTable table)
        throws VirtualTableIncompatibleException {
        final var referenceSchema = extractSchema(table.getReferenceTables()[0]);

        final var appendTable = table.getAppendTable();
        final DataTableValueSchema appendSchema;
        if (appendTable == null) {
            appendSchema = EMPTY;
        } else if (appendTable instanceof AbstractColumnarContainerTable colAppendTable) {
            appendSchema = colAppendTable.getSchema();
        } else {
            throw new VirtualTableIncompatibleException(String.format(
                "Unexpected container table of type '%s' in workflow with Columnar Table Backend detected.",
                appendTable.getClass()));
        }

        final var outputSpec = table.getDataTableSpec();
        return rearrangeSchemas(referenceSchema, appendSchema, outputSpec, table::isFromReferenceTable);
    }

    private static DataTableValueSchema rearrangeSchemas(final DataTableValueSchema referenceSchema,
        final DataTableValueSchema appendSchema, final DataTableSpec outputSpec, final IntPredicate isFromRef) {
        List<ValueFactory<?, ?>> factories = new ArrayList<>(outputSpec.getNumColumns() + 1);
        // add the RowKeyValueFactory
        var rowKeyValueFactory = referenceSchema.getValueFactory(0);
        CheckUtils.checkState(rowKeyValueFactory instanceof RowKeyValueFactory,
            "The first ValueFactory of the reference table was not a RowKeyValueFactory but a '%s'.",
            rowKeyValueFactory.getClass());
        factories.add(rowKeyValueFactory);
        for (int i = 0; i < outputSpec.getNumColumns(); i++) {
            var column = outputSpec.getColumnSpec(i);
            var name = column.getName();
            var schemaContainingFactory = isFromRef.test(i) ? referenceSchema : appendSchema;
            var factory = getValueFactoryForName(schemaContainingFactory, name);
            factories.add(factory);
        }
        return createSchema(outputSpec, factories);
    }

    private static DataTableValueSchema extractSchemaFromJoinedTable(final JoinedTable table)
        throws VirtualTableIncompatibleException {
        return appendSchemas(table.getReferenceTables());
    }

    private static DataTableValueSchema extractSchemaFromSpecReplacerTable(final TableSpecReplacerTable table)
        throws VirtualTableIncompatibleException {
        var refSchema = extractSchema(table.getReferenceTables()[0]);
        List<ValueFactory<?, ?>> factories = IntStream.range(0, refSchema.numColumns())//
            .mapToObj(refSchema::getValueFactory)//
            .collect(Collectors.toList());
        return createSchema(table.getDataTableSpec(), factories);
    }

    private static DataTableValueSchema extractSchemaFromWrappedTable(final WrappedTable table)
        throws VirtualTableIncompatibleException {
        return extractSchema(table.getReferenceTables()[0]);
    }

    private static DataTableValueSchema createSchema(final DataTableSpec sourceSpec,
        final Collection<ValueFactory<?, ?>> factories) {
        return DataTableValueSchemaUtils.create(sourceSpec, factories.toArray(ValueFactory<?, ?>[]::new));
    }

    private VirtualTableSchemaUtils() {

    }
}
