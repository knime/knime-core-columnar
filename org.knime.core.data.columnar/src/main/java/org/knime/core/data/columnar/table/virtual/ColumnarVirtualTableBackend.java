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
 *   Dec 27, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.IntSupplier;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.data.MissingCell;
import org.knime.core.data.RowKey;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.UnmaterializedCell;
import org.knime.core.data.columnar.TableTransformUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarRowContainerUtils;
import org.knime.core.data.columnar.table.ColumnarRowWriteTableSettings;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.ColumnRearrangerUtils;
import org.knime.core.data.container.ColumnRearrangerUtils.RearrangedColumn;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.data.v2.value.VoidRowKeyFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.Node;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.exec.GraphVirtualTableExecutor;
import org.knime.core.table.virtual.spec.MapTransformSpec.DefaultMapperFactory;

import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Handles the construction (and optimization) of virtual tables that are generated via the ExecutionContext in a node.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarVirtualTableBackend {

    private ColumnarVirtualTableBackend() {
    }

    /**
     * @param progressMonitor
     * @param tableIdSupplier
     * @param columnRearranger
     * @param table
     * @param context
     * @return
     * @throws CanceledExecutionException
     * @throws VirtualTableIncompatibleException
     */
    public static VirtualTableExtensionTable rearrange(final ExecutionMonitor progressMonitor,
        final IntSupplier tableIdSupplier, final ColumnRearranger columnRearranger, final BufferedDataTable table,
        final ExecutionContext context) throws CanceledExecutionException, VirtualTableIncompatibleException {
        var tableSpec = table.getDataTableSpec();
        int numColumns = tableSpec.getNumColumns();
        var refTable = ReferenceTables.createReferenceTable(UUID.randomUUID(), table);
        var rearrangedColumns = ColumnRearrangerUtils.extractRearrangedColumns(columnRearranger);
        var cellFactoryColumns = rearrangedColumns.stream()//
            .filter(c -> c.isNewColumn() || c.isConvertedColumn())
            .collect(toList());
        var refTableSchema = refTable.getSchema();
        ReferenceTable[] refTables;
        var appendTableSchema = ColumnarValueSchemaUtils.empty();

        VirtualTable tableToRearrange = asFragmentSource(refTable);
        if (cellFactoryColumns.isEmpty()) {
            refTables = new ReferenceTable[] {refTable};
        } else {
            // TODO implement stateful mapping via iteration (needed once the executor can parallelize)
//            checkForStatefulFactories(cellFactoryColumns);
            checkForDataTypeConverterFactories(cellFactoryColumns);
            var appendTable = createAppendTable(refTable, context, cellFactoryColumns, tableIdSupplier.getAsInt(),
                progressMonitor, table.size());
            refTables = new ReferenceTable[]{refTable, appendTable};
            appendTableSchema = appendTable.getSchema();
            var appendTableSource = asFragmentSource(appendTable)
                // filter out the void row key column
                .filterColumns(IntStream.range(1, cellFactoryColumns.size() + 1).toArray());
            tableToRearrange = tableToRearrange.append(List.of(appendTableSource));
        }
        int appendIdx = 0;//NOSONAR
        int globalIdx = 0;//NOSONAR
        var originalIndices = new int[rearrangedColumns.size()];
        var valueFactories = new ArrayList<ValueFactory<?, ?>>();
        valueFactories.add(refTableSchema.getValueFactory(0));
        int appendOffset = numColumns;
        for (var col : rearrangedColumns) {
            int originalIdx;
            if (col.isNewColumn()) {
                originalIdx = appendOffset + appendIdx;
                valueFactories.add(appendTableSchema.getValueFactory(appendIdx + 1));
                appendIdx++;
            } else {
                originalIdx = col.getOriginalIndex();
                valueFactories.add(refTable.getSchema().getValueFactory(originalIdx + 1));
            }
            originalIndices[globalIdx] = originalIdx;
            globalIdx++;
        }
        var rearranged = TableTransformUtils.createRearrangeTransformations(tableToRearrange, originalIndices,
            numColumns + cellFactoryColumns.size());
        var rearrangedvalueSchema = ColumnarValueSchemaUtils.create(
            ValueSchemaUtils.create(columnRearranger.createSpec(), valueFactories.toArray(ValueFactory[]::new)));
        return new VirtualTableExtensionTable(refTables, rearranged, rearrangedvalueSchema, table.size(),
            tableIdSupplier.getAsInt());

    }

    private static void checkForStatefulFactories(final List<RearrangedColumn> cellFactoryColumns)
        throws VirtualTableIncompatibleException {
        if (cellFactoryColumns.stream()//
                .map(RearrangedColumn::getCellFactory)//
                .anyMatch(CellFactory::hasState)) {
            throw new VirtualTableIncompatibleException(
                "Stateful CellFactory detected. Such factories are currently not supported with virtual tables.");
        }
    }

    private static void checkForDataTypeConverterFactories(final List<RearrangedColumn> cellFactoryColumns)
        throws VirtualTableIncompatibleException {
        if (cellFactoryColumns.stream()//
            .anyMatch(RearrangedColumn::isConvertedColumn)) {
            throw new VirtualTableIncompatibleException(
                "DataCellTypeConverters are not supported by the Columnar Table Backend.");
        }
    }

    private static VirtualTable asFragmentSource(final ReferenceTable table) {
        return new VirtualTable(table.getId(), table.getSchema());
    }

    private static ReferenceTable createAppendTable(final ReferenceTable table, final ExecutionContext context,
        final List<RearrangedColumn> cellFactoryColumns, final int tableId, final ExecutionMonitor monitor, final long size)
        throws VirtualTableIncompatibleException, CanceledExecutionException {
        var schema = table.getSchema();
        var inputValueFactories = new GenericValueFactory[schema.numColumns()];
        Arrays.setAll(inputValueFactories, i -> new GenericValueFactory(schema.getValueFactory(i)));
        var fsHandler = Node.invokeGetFileStoreHandler(context);
        var sourceTable = table.getVirtualTable();
        var mapBuilder = new MapBuilder(sourceTable, schema, fsHandler);
        ArrayList<VirtualTable> maps = cellFactoryColumns.stream()//
            .map(RearrangedColumn::getCellFactory)//
            .map(mapBuilder::map)//
            .collect(Collectors.toCollection(ArrayList::new));
        var appended = sourceTable
                .filterColumns(0)// keep the row key for progress reporting below
                .append(maps);
        var progressFactory = cellFactoryColumns.get(0).getCellFactory();

        var appendContainerSchema = appendSchemas(VoidRowKeyFactory.INSTANCE, maps);
        // the cursor contains the original row key for progress reporting while the container has a void row key
        // since it is filtered out anyway when the table is appended to the input table
        var appendCursorSchema = appendSchemas(schema.getValueFactory(0), maps);
        final var executor = new GraphVirtualTableExecutor(appended.getProducingTransform());
        try (final var container = createAppendContainer(appendContainerSchema, context, tableId);
                var writeCursor = container.createCursor();
                var mappedRows = executor.execute(table.getSources()).get(0);
                var readCursor = mappedRows.createCursor()) {
            var accessRow = readCursor.access();
            var rowRead = VirtualTableUtils.createRowRead(appendCursorSchema, accessRow,
                new DefaultColumnSelection(mappedRows.getSchema().numColumns()));
            for (long r = 0; readCursor.forward(); r++) {
                writeCursor.forward().setFrom(rowRead);
                progressFactory.setProgress(r + 1, size, new RowKey(rowRead.getRowKey().getString()), monitor);
                monitor.checkCanceled();
            }
            var appendTable = container.finish();
            // TODO call special AbstractCellFactory#afterProcessing where necessary
            return ReferenceTables.createReferenceTable(UUID.randomUUID(), appendTable);
        } catch (CanceledExecutionException canceledException) {
            // this stunt is necessary because ColumnarRowContainerUtils.create throws Exception
            throw canceledException;
        } catch (Exception ex) {
            throw new VirtualTableIncompatibleException("Failed to create append table.", ex);
        }
    }

    private static RowContainer createAppendContainer(final ColumnarValueSchema schema, final ExecutionContext context,
        final int tableId) throws Exception {
        var dataContainerSettings = DataContainerSettings.getDefault();
        var columnarContainerSettings =
            new ColumnarRowWriteTableSettings(true, dataContainerSettings.getMaxDomainValues(), false, false);
        return ColumnarRowContainerUtils.create(context, tableId, schema, columnarContainerSettings);
    }

    private static ColumnarValueSchema appendSchemas(final ValueFactory<?, ?> rowKeyValueFactory,
        final Collection<VirtualTable> tables) {
        List<ColumnarValueSchema> schemas = tables.stream()//
            .map(VirtualTable::getSchema)//
            .map(ColumnarValueSchema.class::cast)//
            .collect(Collectors.toList());
        var tableSpec = new DataTableSpec(schemas.stream()//
            .map(ColumnarValueSchema::getSourceSpec)//
            .flatMap(DataTableSpec::stream)//
            .toArray(DataColumnSpec[]::new)//
        );
        var valueFactories = Stream.concat(//
            Stream.of(rowKeyValueFactory), //
            schemas.stream()//
                .flatMap(s -> IntStream.range(0, s.numColumns()).mapToObj(s::getValueFactory))//
        ).toArray(ValueFactory[]::new);
        return ColumnarValueSchemaUtils.create(ValueSchemaUtils.create(tableSpec, valueFactories));
    }

    private static class MapBuilder {

        private final GenericValueFactory[] m_inputValueFactories;

        private final IWriteFileStoreHandler m_fsHandler;

        private final int[] m_inputIndices;

        private final VirtualTable m_sourceTable;

        private final int m_numColumnsInInputTable;

        MapBuilder(final VirtualTable sourceTable, final ColumnarValueSchema schema,
            final IWriteFileStoreHandler fsHandler) {
            m_sourceTable = sourceTable;
            m_fsHandler = fsHandler;
            m_inputValueFactories = new GenericValueFactory[schema.numColumns()];
            Arrays.setAll(m_inputValueFactories, i -> new GenericValueFactory(schema.getValueFactory(i)));
            m_inputIndices = IntStream.range(0, m_inputValueFactories.length).toArray();
            m_numColumnsInInputTable = m_inputValueFactories.length - 1; // the row key is not counted towards the cols
        }

        VirtualTable map(final CellFactory factory) {
            var outputSpecs = factory.getColumnSpecs();
            ValueFactory<?, ?>[] valueFactories = Stream.of(outputSpecs)//
                .map(DataColumnSpec::getType)//
                .map(t -> ValueFactoryUtils.getValueFactory(t, m_fsHandler))//
                .toArray(ValueFactory[]::new);
            GenericValueFactory[] outputValueFactories = Stream.of(valueFactories)//
                .map(GenericValueFactory::new)//
                .toArray(GenericValueFactory[]::new);

            var requiredColumnsMapper = createRequiredColumnsMapper(factory);
            var inputFactories = Stream.concat(//
                Stream.of(m_inputValueFactories[0]), // the row key is always included
                IntStream.range(0, m_inputValueFactories.length - 1)//
                    .filter(i -> requiredColumnsMapper.applyAsInt(i) > -1) // column is required
                    .map(i -> i + 1) // account for row key column
                    .mapToObj(i -> m_inputValueFactories[i]))//
                .toArray(GenericValueFactory[]::new);
            var cellFactoryMap = new CellFactoryMap(inputFactories, outputValueFactories,
                factory, requiredColumnsMapper, m_numColumnsInInputTable);
            var valueSchema = ValueSchemaUtils.create(new DataTableSpec(factory.getColumnSpecs()), valueFactories);
            var outputSchema = ColumnarValueSchemaUtils.create(valueSchema);
            return m_sourceTable.map(getColumnIndices(factory),
                new DefaultMapperFactory(outputSchema, cellFactoryMap::createMapper));
        }

        private static IntUnaryOperator createRequiredColumnsMapper(final CellFactory cellFactory) {
            var requiredCols = cellFactory.getRequiredColumns();
            if (requiredCols.isPresent()) {
                var array = requiredCols.get();
                var mapper = new TIntIntHashMap(array.length, 0.7f, -1, -1);
                IntStream.range(0, array.length)//
                    .forEach(i -> mapper.put(array[i], i));
                return mapper::get;
            } else {
                return IntUnaryOperator.identity();
            }
        }

        private int[] getColumnIndices(final CellFactory factory) {
            var requiredColumns = factory.getRequiredColumns();
            if (requiredColumns.isPresent()) {
                return IntStream.concat(IntStream.of(0), // the row key column is always included
                    IntStream.of(requiredColumns.get())//
                        .map(i -> i + 1)// required columns does not contain the row key
                ).toArray();
            } else {
                return m_inputIndices;
            }
        }
    }

    private static class CellFactoryMap {

        private final GenericValueFactory[] m_readValueFactories;

        private final GenericValueFactory[] m_writeValueFactories;

        private final CellFactory m_cellFactory;

        private final IntUnaryOperator m_requiredColumnMapper;

        private final int m_numInputColumns;

        CellFactoryMap(final GenericValueFactory[] inputValueFactories,
            final GenericValueFactory[] outputValueFactories, final CellFactory cellFactory,
            final IntUnaryOperator requiredColumnMapper, final int numInputColumns) {
            m_readValueFactories = inputValueFactories;
            m_writeValueFactories = outputValueFactories;
            m_cellFactory = cellFactory;
            m_requiredColumnMapper = requiredColumnMapper;
            m_numInputColumns = numInputColumns;
        }

        Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var rowKey = (RowKeyValue)m_readValueFactories[0].createReadValue(inputs[0]);
            var readValues = IntStream.range(1, inputs.length)//
                .mapToObj(i -> new NullableReadValue(m_readValueFactories[i].createReadValue(inputs[i]), inputs[i]))
                .toArray(NullableReadValue[]::new);
            var writeValues = new NullableWriteValue[outputs.length];
            Arrays.setAll(writeValues,
                i -> new NullableWriteValue<>(m_writeValueFactories[i].createWriteValue(outputs[i]), outputs[i]));
            final var cellConsumer = new CellConsumer(writeValues);
            final var rowSupplier = new RowSupplier(rowKey, readValues, m_numInputColumns, m_requiredColumnMapper);
            return () -> cellConsumer.accept(m_cellFactory.getCells(rowSupplier.getRow()));
        }

    }

    private static final class RowSupplier {

        private final NullableReadValue[] m_readValues;

        private final RowKeyValue m_rowKey;

        private final IntUnaryOperator m_requiredColumnMapper;

        // reuse same cell array since a new one will be created in DefaultRow anyway
        private final DataCell[] m_cells;

        RowSupplier(final RowKeyValue rowKey, final NullableReadValue[] readValues, final int numInputColumns,
            final IntUnaryOperator requiredColumnMapper) {
            m_readValues = readValues;
            m_requiredColumnMapper = requiredColumnMapper;
            m_rowKey = rowKey;
            m_cells = new DataCell[numInputColumns];
        }

        DataRow getRow() {
            var rowKey = new RowKey(m_rowKey.getString());
            Arrays.setAll(m_cells, this::getCell);
            return new DefaultRow(rowKey, m_cells);
        }

        private DataCell getCell(final int i) {
            var mappedIdx = m_requiredColumnMapper.applyAsInt(i);
            if (mappedIdx > -1) {
                return m_readValues[mappedIdx].getDataCell();
            } else {
                return UnmaterializedCell.getInstance();
            }
        }

    }

    private static final class CellConsumer {
        private final NullableWriteValue<?>[] m_writeValues;

        CellConsumer(final NullableWriteValue<?>[] writeValues) {
            m_writeValues = writeValues;
        }

        void accept(final DataCell[] cells) {
            for (int i = 0; i < m_writeValues.length; i++) {//NOSONAR
                m_writeValues[i].setDataCell(cells[i]);
            }
        }
    }

    private static final class NullableWriteValue<D extends DataValue> {

        private final WriteValue<D> m_writeValue;

        private final WriteAccess m_writeAccess;

        NullableWriteValue(final WriteValue<D> writeValue, final WriteAccess writeAccess) {
            m_writeValue = writeValue;
            m_writeAccess = writeAccess;
        }

        @SuppressWarnings("unchecked")
        void setDataCell(final DataCell cell) {
            if (cell.isMissing()) {
                m_writeAccess.setMissing();
            } else {
                m_writeValue.setValue((D)cell);
            }
        }

    }

    private static final class NullableReadValue {

        private static final MissingCell MISSING = new MissingCell("");

        private final ReadValue m_readValue;

        private final ReadAccess m_readAccess;

        NullableReadValue(final ReadValue readValue, final ReadAccess readAccess) {
            m_readValue = readValue;
            m_readAccess = readAccess;
        }

        DataCell getDataCell() {
            if (m_readAccess.isMissing()) {
                return MISSING;
            } else {
                return m_readValue.getDataCell();
            }
        }
    }

    /**
     * Helper class that hides the generics of {@link ValueFactory} from clients that can't possibly no them. Requires
     * unsafe casts to create the {@link ReadValue Read-} and {@link WriteValue WriteValues}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private static class GenericValueFactory {
        private final ValueFactory<?, ?> m_valueFactory;

        GenericValueFactory(final ValueFactory<?, ?> valueFactory) {
            m_valueFactory = valueFactory;
        }

        ReadValue createReadValue(final ReadAccess access) {
            return createReadValue(m_valueFactory, access);
        }

        @SuppressWarnings("unchecked")
        private static <R extends ReadAccess> ReadValue createReadValue(final ValueFactory<R, ?> valueFactory,
            final ReadAccess access) {
            return valueFactory.createReadValue((R)access);
        }

        WriteValue<?> createWriteValue(final WriteAccess access) {
            return createWriteValue(m_valueFactory, access);
        }

        @SuppressWarnings("unchecked")
        private static <W extends WriteAccess> WriteValue<?> createWriteValue(final ValueFactory<?, W> valueFactory,
            final WriteAccess access) {
            return valueFactory.createWriteValue((W)access);
        }
    }
}
