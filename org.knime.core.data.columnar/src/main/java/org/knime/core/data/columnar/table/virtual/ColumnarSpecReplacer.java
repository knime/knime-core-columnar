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
 *   Mar 9, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import static org.knime.core.data.v2.ValueFactoryUtils.areEqual;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.virtual.ValueFactoryMapperFactory.ColumnMapperFactory;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.Node;
import org.knime.core.node.util.CheckUtils;

/**
 * Replaces the spec of the input table.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarSpecReplacer {

    private final IntSupplier m_tableIDSupplier;

    private final IWriteFileStoreHandler m_fsHandler;

    /**
     * Constructor
     *
     * @param exec needed to get a filestore handler
     * @param tableIDSupplier provides IDs for new tables
     */
    public ColumnarSpecReplacer(final ExecutionContext exec, final IntSupplier tableIDSupplier) {
        m_tableIDSupplier = tableIDSupplier;
        m_fsHandler = Node.invokeGetFileStoreHandler(exec);
    }

    /**
     * Replaces the spec of the input table. For each column the type in the outputSpec must be the same or a super-type
     * of the type in the input table.
     *
     * @param table to replace the spec of
     * @param outputSpec the new spec
     * @return the table with the replaced spec
     * @throws VirtualTableIncompatibleException if the input table is not compatible with virtual tables
     */
    public VirtualTableExtensionTable replaceSpec(final BufferedDataTable table, final DataTableSpec outputSpec)
        throws VirtualTableIncompatibleException {
        var inputSpec = table.getDataTableSpec();
        CheckUtils.checkArgument(inputSpec.getNumColumns() == outputSpec.getNumColumns(),
            "Table specs have different lengths: %s vs. %s", inputSpec.getNumColumns(), outputSpec.getNumColumns());
        var referenceTable = ReferenceTables.createReferenceTable(table);

        var outputSchema = ColumnarValueSchemaUtils.create(outputSpec, RowKeyType.CUSTOM, m_fsHandler);

        var inputSchema = referenceTable.getSchema();
        var mappings = determineMappings(inputSchema, outputSchema);
        var sourceFragment = new ColumnarVirtualTable(referenceTable.getId(), inputSchema, true);
        ColumnarVirtualTable outputTable;
        if (mappings.isEmpty()) {
            outputTable = sourceFragment.replaceSchema(outputSchema);
        } else {
            outputTable = upcast(sourceFragment, mappings, m_fsHandler);
        }

        return new VirtualTableExtensionTable(new ReferenceTable[]{referenceTable}, outputTable, table.size(),
            m_tableIDSupplier.getAsInt());
    }

    /**
     * @param table to map
     * @param mappings applying the individual maps
     * @param fsHandler used to initialize FileStoreAwareValueFactories
     * @return the input table with the upcasted columns
     */
    static ColumnarVirtualTable upcast(final ColumnarVirtualTable table, final List<ColumnMapping> mappings,
        final IWriteFileStoreHandler fsHandler) {
        var inputSchema = table.getSchema();
        var indexFilters = createIndexFilters(mappings, inputSchema.numColumns());
        var mappedColumns =
            table.map(new ValueFactoryMapperFactory(mappings.stream().map(ColumnMapping::createMapperFactory).toList(),
                fsHandler.getDataRepository()), indexFilters.mappedColumns());
        return table.selectColumns(indexFilters.unmappedColumns())//
            .append(List.of(mappedColumns))//
            .selectColumns(indexFilters.mergePermutation());
    }

    record ColumnMapping(int columnIndex, DataColumnSpec outputSpec, UntypedValueFactory inputValueFactory,
        UntypedValueFactory outputValueFactory) {
        ColumnMapperFactory createMapperFactory() {
            return new ColumnMapperFactory(outputSpec, inputValueFactory, outputValueFactory);
        }
    }

    private static IndexFilters createIndexFilters(final List<ColumnMapping> mappings, final int numColumns) {
        var mappedColumns = new int[mappings.size()];
        var numUnmapped = numColumns - mappings.size();
        var unmappedColumns = new int[numUnmapped];
        var mergePermutation = new int[numColumns];
        int mappedIndex = 0;
        int unmappedIndex = 0;
        var mappingIterator = mappings.iterator();
        int currentMappingIndex = -1;
        for (int i = 0; i < numColumns; i++) {
            if (currentMappingIndex < i && mappingIterator.hasNext()) {
                currentMappingIndex = mappingIterator.next().columnIndex();
            }
            if (currentMappingIndex == i) {
                mappedColumns[mappedIndex] = i;
                mergePermutation[i] = numUnmapped + mappedIndex;
                mappedIndex++;
            } else {
                unmappedColumns[unmappedIndex] = i;
                mergePermutation[i] = unmappedIndex;
                unmappedIndex++;
            }
        }
        return new IndexFilters(mappedColumns, unmappedColumns, mergePermutation);

    }

    record IndexFilters(int[] mappedColumns, int[] unmappedColumns, int[] mergePermutation) {
    }

    private static List<ColumnMapping> determineMappings(final ColumnarValueSchema inputSchema,
        final ColumnarValueSchema outputSchema) {
        var mappings = new ArrayList<ColumnMapping>();
        CheckUtils.checkArgument(areEqual(inputSchema.getValueFactory(0), outputSchema.getValueFactory(0)),
            "The RowID ValueFactories must match.");
        for (int i = 1; i < inputSchema.numColumns(); i++) {
            var columnIndex = i - 1; // because of the RowID
            var inputColumn = inputSchema.getSourceSpec().getColumnSpec(columnIndex);
            var inputType = inputColumn.getType();
            var outputColumn = outputSchema.getSourceSpec().getColumnSpec(columnIndex);
            var outputType = outputColumn.getType();
            var inputValueFactory = inputSchema.getValueFactory(i);
            var outputValueFactory = outputSchema.getValueFactory(i);
            if (outputType.equals(inputType)) {
                continue;
            } else if (outputType.isASuperTypeOf(inputType) && !areEqual(inputValueFactory, outputValueFactory)) {
                var mapping = new ColumnMapping(i, outputColumn, new UntypedValueFactory(inputValueFactory),
                    new UntypedValueFactory(outputValueFactory));
                mappings.add(mapping);
            } else {
                // the row-based backend does not validate the types of the new spec but here we have to do it
                // because we have to ensure that we have the correct ValueFactories
                throw new IllegalArgumentException(
                    "The output type %s in column %s must be the same or a super type of the input type %s."
                        .formatted(outputType, columnIndex, inputType));
            }
        }
        return mappings;
    }

}
