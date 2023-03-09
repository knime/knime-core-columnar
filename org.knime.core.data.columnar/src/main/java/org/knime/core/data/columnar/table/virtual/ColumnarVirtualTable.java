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
 *   Mar 3, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTableSpecCreator;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.v2.RowKeyValueFactory;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

/**
 * Equivalent to {@link VirtualTable} with the difference that {@link #getSchema()} returns a
 * {@link ColumnarValueSchema}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
final class ColumnarVirtualTable {

    private final VirtualTable m_virtualTable;

    private final ColumnarValueSchema m_valueSchema;

    ColumnarVirtualTable(final UUID sourceIdentifier, final ColumnarValueSchema schema, final boolean lookahead) {
        this(new VirtualTable(sourceIdentifier, schema, lookahead), schema);
    }

    ColumnarVirtualTable(final VirtualTable virtualTable, final ColumnarValueSchema schema) {
        m_valueSchema = schema;
        m_virtualTable = virtualTable;
    }

    ColumnarValueSchema getSchema() {
        return m_valueSchema;
    }

    VirtualTable getVirtualTable() {
        return m_virtualTable;
    }

    ColumnarVirtualTable append(final List<ColumnarVirtualTable> tables) {
        var tablesWithoutRowIDs = tables.stream()//
            .map(ColumnarVirtualTable::filterRowID)//
            .toList();
        var schema = appendSchemas(collectSchemas(tablesWithoutRowIDs));
        var virtualTables = tablesWithoutRowIDs.stream()//
            .map(ColumnarVirtualTable::getVirtualTable)//
            .collect(Collectors.toList());
        var virtualTable = m_virtualTable.append(virtualTables);
        return new ColumnarVirtualTable(virtualTable, schema);
    }

    private static ColumnarVirtualTable filterRowID(final ColumnarVirtualTable table) {
        var schema = table.getSchema();
        if (ColumnarValueSchemaUtils.hasRowID(schema)) {
            var virtualTableWithoutRowID =
                table.getVirtualTable().filterColumns(IntStream.range(1, schema.numColumns()).toArray());
            var schemaWithoutRowID = ColumnarValueSchemaUtils.create(schema.getSourceSpec(), IntStream
                .range(1, schema.numColumns()).mapToObj(schema::getValueFactory).toArray(ValueFactory<?, ?>[]::new));
            return new ColumnarVirtualTable(virtualTableWithoutRowID, schemaWithoutRowID);
        } else {
            return table;
        }
    }

    private static ColumnarValueSchema appendSchemas(final List<ColumnarValueSchema> schemas) {
        var valueFactories = schemas.stream()//
            .flatMap(ColumnarVirtualTable::valueFactoryStream)//
            .toArray(ValueFactory<?, ?>[]::new);
        var spec = new DataTableSpec(//
            schemas.stream()//
                .map(ColumnarValueSchema::getSourceSpec)//
                .flatMap(DataTableSpec::stream)//
                .toArray(DataColumnSpec[]::new)//
        );
        return ColumnarValueSchemaUtils.create(spec, valueFactories);
    }

    ColumnarVirtualTable map(final ColumnarMapperFactory mapperFactory, final int... columnIndices) {
        var virtualTable = m_virtualTable.map(columnIndices, mapperFactory);
        var schema = mapperFactory.getOutputSchema();
        return new ColumnarVirtualTable(virtualTable, schema);
    }

    /**
     * A {@link MapperFactory} whose {@link #getOutputSchema()} method returns a {@link ColumnarValueSchema}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface ColumnarMapperFactory extends MapperFactory {
        @Override
        ColumnarValueSchema getOutputSchema();
    }

    ColumnarVirtualTable permute(final int... permutation) {
        var schema = permute(m_valueSchema, permutation);
        var virtualTable = m_virtualTable.permute(permutation);
        return new ColumnarVirtualTable(virtualTable, schema);
    }

    private static ColumnarValueSchema permute(final ColumnarValueSchema schema, final int[] permutation) {
        var hasRowID = hasRowID(schema);
        if (hasRowID) {
            CheckUtils.checkArgument(permutation[0] == 0, "The RowID must stay at the first position of the table.");
        }
        var valueFactories = IntStream.of(permutation)//
            .mapToObj(schema::getValueFactory)//
            .toArray(ValueFactory<?, ?>[]::new);
        var originalSpec = schema.getSourceSpec();
        var specCreator = new DataTableSpecCreator(originalSpec);
        specCreator.dropAllColumns();
        var permutationStream = IntStream.of(permutation);
        if (hasRowID) {
            permutationStream = permutationStream.skip(1).map(i -> i - 1); // the rowID is not part of the DataTableSpec
        }
        specCreator.addColumns(permutationStream.mapToObj(originalSpec::getColumnSpec).toArray(DataColumnSpec[]::new));

        return createColumnarValueSchema(valueFactories, specCreator.createSpec());
    }

    /**
     * Filters columns.
     *
     * @param columnIndices the columns to include
     * @return the filtered table
     */
    ColumnarVirtualTable filterColumns(final int... columnIndices) {
        var virtualTable = m_virtualTable.filterColumns(columnIndices);
        var schema = filterColumns(m_valueSchema, columnIndices);
        return new ColumnarVirtualTable(virtualTable, schema);
    }

    private static ColumnarValueSchema filterColumns(final ColumnarValueSchema schema, final int[] columnIndices) {
        var valueFactories = IntStream.of(columnIndices)//
            .mapToObj(schema::getValueFactory)//
            .toArray(ValueFactory<?, ?>[]::new);

        var originalSpec = schema.getSourceSpec();
        var indices = IntStream.of(columnIndices);
        if (hasRowID(schema)) {
            // the RowID is not part of the spec
            indices = indices.filter(i -> i > 0).map(i -> i - 1);
        }
        var specCreator = new DataTableSpecCreator(originalSpec);
        specCreator.dropAllColumns();
        specCreator.addColumns(indices.mapToObj(originalSpec::getColumnSpec).toArray(DataColumnSpec[]::new));
        return createColumnarValueSchema(valueFactories, specCreator.createSpec());
    }

    private static boolean hasRowID(final ColumnarValueSchema schema) {
        return schema.getValueFactory(0) instanceof RowKeyValueFactory;
    }

    private static ColumnarValueSchema createColumnarValueSchema(final ValueFactory<?, ?>[] valueFactories,
        final DataTableSpec spec) {
        return ColumnarValueSchemaUtils.create(ValueSchemaUtils.create(spec, valueFactories));
    }

    private static Stream<ValueFactory<?, ?>> valueFactoryStream(final ColumnarValueSchema schema) {
        return IntStream.range(0, schema.numColumns()).mapToObj(schema::getValueFactory);
    }

    private List<ColumnarValueSchema> collectSchemas(final List<ColumnarVirtualTable> tables) {
        return Stream.concat(//
            Stream.of(m_valueSchema), //
            tables.stream().map(ColumnarVirtualTable::getSchema))//
            .collect(Collectors.toList());
    }

}
