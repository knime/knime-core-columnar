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
import java.util.Optional;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.virtual.TableCasterFactory.CastOperation;
import org.knime.core.data.columnar.table.virtual.TableCasterFactory.ColumnCasterFactory;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.schema.DataTableValueSchemaUtils;
import org.knime.core.data.v2.schema.ValueSchema;
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
     * Replaces the spec of the input table. For each column the output type must be related to the input type, i.e. the
     * output type must be a supertype of the input type or vice-versa.
     *
     * @param table to replace the spec of
     * @param outputSpec the new spec
     * @return the table with the replaced spec
     * @throws VirtualTableIncompatibleException if the input table is not compatible with virtual tables
     * @throws IllegalArgumentException if the input and output type of any column are not related
     */
    public VirtualTableExtensionTable replaceSpec(final BufferedDataTable table, final DataTableSpec outputSpec)
        throws VirtualTableIncompatibleException {
        var inputSpec = table.getDataTableSpec();
        CheckUtils.checkArgument(inputSpec.getNumColumns() == outputSpec.getNumColumns(),
            "Table specs have different lengths: %s vs. %s", inputSpec.getNumColumns(), outputSpec.getNumColumns());
        var referenceTable = ReferenceTables.createReferenceTable(table);


        var inputSchema = referenceTable.getSchema();
        var outputSchema = DataTableValueSchemaUtils.create(outputSpec, RowKeyType.CUSTOM, m_fsHandler);
        var casts = determineCasts(inputSchema, outputSchema);

        var sourceFragment = new ColumnarVirtualTable(referenceTable.getId(), inputSchema, true); // TODO (TP): why not just referenceTable.getVirtualTable() ???
        var outputTable = casts.isEmpty() ? sourceFragment : cast(sourceFragment, casts, m_fsHandler);

        return new VirtualTableExtensionTable(new ReferenceTable[]{referenceTable},
            outputTable.updateSchema(outputSpec),
            outputSpec,
            table.size(), m_tableIDSupplier.getAsInt());
    }

    /**
     * @param table to cast
     * @param casts applying the individual casts
     * @param fsHandler used to initialize FileStoreAwareValueFactories
     * @return the input table with the casted columns
     */
    static ColumnarVirtualTable cast(final ColumnarVirtualTable table, final List<ColumnCast> casts,
        final IWriteFileStoreHandler fsHandler) {

        final int numColumns = table.getSchema().numColumns();
        final int[] selection = IntStream.range(0, numColumns).toArray();
        final int[] castedColumns = casts.stream().mapToInt(ColumnCast::columnIndex).toArray();
        for (int i = 0; i < castedColumns.length; i++) {
            selection[castedColumns[i]] = numColumns + i;
        }

        final var columnCasterFactories = casts.stream().map(ColumnCast::createCasterFactory).toList();
        final var mapperFactory = new TableCasterFactory(columnCasterFactories, fsHandler.getDataRepository());
        return table.appendMap(mapperFactory, castedColumns).selectColumns(selection);
    }

    record ColumnCast(int columnIndex, DataColumnSpec outputSpec, ValueFactory<?, ?> inputValueFactory,
        ValueFactory<?, ?> outputValueFactory, CastOperation castOperation) {
        ColumnCasterFactory createCasterFactory() {
            return new ColumnCasterFactory(outputSpec, inputValueFactory, outputValueFactory, castOperation);
        }
    }

    private static List<ColumnCast> determineCasts(final ValueSchema inputSchema,
        final ValueSchema outputSchema) {
        var casts = new ArrayList<ColumnCast>();
        CheckUtils.checkArgument(areEqual(inputSchema.getValueFactory(0), outputSchema.getValueFactory(0)),
            "The RowID ValueFactories must match.");
        for (int i = 1; i < inputSchema.numColumns(); i++) {
            var inputColumn = inputSchema.getDataColumnSpec(i);
            var inputType = inputColumn.getType();
            var outputColumn = outputSchema.getDataColumnSpec(i);
            var outputType = outputColumn.getType();
            var inputValueFactory = inputSchema.getValueFactory(i);
            var outputValueFactory = outputSchema.getValueFactory(i);
            final int finalI = i;
            determineCastType(inputType, outputType).ifPresent(
                c -> casts.add(new ColumnCast(finalI, outputColumn, inputValueFactory, outputValueFactory, c)));
        }
        return casts;
    }

    private static Optional<CastOperation> determineCastType(final DataType inputType, final DataType outputType) {
        if (inputType.equals(outputType)) {
            return Optional.empty();
        }

        if (inputType.isASuperTypeOf(outputType)) {
            return Optional.of(CastOperation.DOWNCAST);
        } else if (outputType.isASuperTypeOf(inputType)) {
            return Optional.of(CastOperation.UPCAST);
        } else {
            throw new IllegalArgumentException(
                ("The input type '%s' and the output type '%s' are not related to each other "
                    + "and therefore casting is not possible.").formatted(inputType, outputType));
        }

    }

}
