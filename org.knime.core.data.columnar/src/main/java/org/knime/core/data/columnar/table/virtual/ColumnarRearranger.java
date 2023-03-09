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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataCellTypeConverter;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.MissingCell;
import org.knime.core.data.RowKey;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarRowContainerUtils;
import org.knime.core.data.columnar.table.ColumnarRowWriteTableSettings;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperFactory;
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
import org.knime.core.data.v2.RowKeyValueFactory;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.value.VoidRowKeyFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.Node;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.virtual.exec.GraphVirtualTableExecutor;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

/**
 * Handles the construction (and optimization) of virtual tables that are generated via the ExecutionContext in a node.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarRearranger {

    private final ExecutionContext m_context;

    private final IWriteFileStoreHandler m_fsHandler;

    private final IntSupplier m_tableIdSupplier;

    /**
     * Constructor.
     *
     * @param context of the ColumnarRearranger
     * @param tableIdSupplier provides IDs for new tables
     */
    public ColumnarRearranger(final ExecutionContext context, final IntSupplier tableIdSupplier) {
        m_context = context;
        m_fsHandler = Node.invokeGetFileStoreHandler(context);
        m_tableIdSupplier = tableIdSupplier;
    }

    /**
     * Transforms the input table according to the ColumnRearranger.
     *
     * @param progressMonitor
     * @param columnRearranger
     * @param table the table to transform
     * @return the transforms table
     * @throws CanceledExecutionException
     * @throws VirtualTableIncompatibleException
     */
    public VirtualTableExtensionTable transform(final ExecutionMonitor progressMonitor,
        final ColumnRearranger columnRearranger, final BufferedDataTable table)
        throws CanceledExecutionException, VirtualTableIncompatibleException {
        var refTable = ReferenceTables.createReferenceTable(UUID.randomUUID(), table);
        var rearrangedColumns = ColumnRearrangerUtils.extractRearrangedColumns(columnRearranger);
        List<ReferenceTable> refTables = new ArrayList<>();
        refTables.add(refTable);

        var newColumns = rearrangedColumns.stream()//
            .filter(ColumnarRearranger::isAppendedColumn)//
            .toList();
        var existingColumns = rearrangedColumns.stream()//
            .filter(c -> !isAppendedColumn(c))//
            // ensure that the existing columns are in the order they are in the input table
            .sorted((l, r) -> Integer.compare(l.getOriginalIndex(), r.getOriginalIndex()))//
            .toList();
        var tableToRearrange = asFragmentSource(refTable).filterColumns(//
            addRowIDIndex(//
                existingColumns.stream()//
                    .mapToInt(RearrangedColumn::getOriginalIndex)//
            ).toArray()//
        );

        if (!newColumns.isEmpty()) {
            var appendTable = createAppendTable(refTable, newColumns, progressMonitor, table.size());
            refTables.add(appendTable);
            var appendTableSource = asFragmentSource(appendTable)
                // filter out the void row key column
                .filterColumns(IntStream.range(1, newColumns.size() + 1).toArray());
            tableToRearrange = tableToRearrange.append(List.of(appendTableSource));
        }

        var rearrangedTable = rearrange(newColumns, existingColumns, tableToRearrange);

        return new VirtualTableExtensionTable(refTables.toArray(ReferenceTable[]::new),
            rearrangedTable.getVirtualTable(), rearrangedTable.getSchema(), table.size(), m_tableIdSupplier.getAsInt());

    }

    private static ColumnarVirtualTable rearrange(final List<RearrangedColumn> newColumns,
        final List<RearrangedColumn> existingColumns, final ColumnarVirtualTable tableToRearrange) {
        int[] permutation = new int[tableToRearrange.getSchema().numColumns()];
        int i = 1;
        for (var col : existingColumns) {
            permutation[col.getGlobalIndex() + 1] = i;
            i++;
        }
        for (var col : newColumns) {
            permutation[col.getGlobalIndex() + 1] = i;
            i++;
        }
        return tableToRearrange.permute(permutation);
    }

    private static IntStream addRowIDIndex(final IntStream indicesWithoutRowID) {
        return IntStream.concat(IntStream.of(0), indicesWithoutRowID.map(i -> i + 1));
    }

    private static boolean isAppendedColumn(final RearrangedColumn column) {
        return column.isNewColumn() || column.isConvertedColumn();
    }

    private static ColumnarVirtualTable asFragmentSource(final ReferenceTable table) {
        return new ColumnarVirtualTable(table.getId(), table.getSchema(), true);
    }

    private ReferenceTable createAppendTable(final ReferenceTable table, final List<RearrangedColumn> columnsToMap,
        final ExecutionMonitor monitor, final long size)
        throws VirtualTableIncompatibleException, CanceledExecutionException {
        var schema = table.getSchema();
        var inputValueFactories = new GenericValueFactory[schema.numColumns()];
        Arrays.setAll(inputValueFactories, i -> new GenericValueFactory(schema.getValueFactory(i)));
        var sourceTable = new ColumnarVirtualTable(table.getVirtualTable(), schema);

        var columnsToConvert = columnsToMap.stream()//
            .filter(RearrangedColumn::isConvertedColumn)//
            .collect(Collectors.toList());
        var mappedColumns = new LinkedHashMap<RearrangedColumn, ColumnarVirtualTable>();
        if (!columnsToConvert.isEmpty()) {
            var conversionResult = convertColumns(sourceTable, columnsToConvert);
            sourceTable = conversionResult.tableWithConvertedColumns();
            mappedColumns.putAll(conversionResult.convertedColumns());
        }

        var columnsToCreate = columnsToMap.stream()//
            .filter(c -> c.isNewColumn())//
            .collect(Collectors.toList());

        Progress progress;
        if (columnsToCreate.isEmpty()) {
            progress = new SimpleProgress(monitor, size);
        } else {
            mappedColumns.putAll(createColumns(columnsToCreate, sourceTable));
            progress = new CellFactoryProgress(monitor, size, columnsToCreate.get(0).getCellFactory());
        }

        var mappedColumnList = mappedColumns.entrySet().stream()//
            .sorted((l, r) -> Integer.compare(l.getKey().getNewColIndex(), r.getKey().getNewColIndex()))//
            .map(Map.Entry::getValue)//
            .toList();

        ColumnarVirtualTable appendedColumns =
            mappedColumnList.get(0).append(mappedColumnList.subList(1, mappedColumnList.size()));
        return materializeAppendTable(table, progress, sourceTable, appendedColumns);
    }

    private Map<RearrangedColumn, ColumnarVirtualTable> createColumns(final List<RearrangedColumn> columnsToCreate,
        final ColumnarVirtualTable table) {
        var mapBuilder = new MapBuilder(table);
        var cellFactoryTables = new HashMap<CellFactory, ColumnarVirtualTable>();
        var createdColumns = new LinkedHashMap<RearrangedColumn, ColumnarVirtualTable>();
        for (var col : columnsToCreate) {
            var cellFactoryTable = cellFactoryTables.computeIfAbsent(col.getCellFactory(), mapBuilder::map);
            var createdCol = cellFactoryTable.filterColumns(col.getIndexInFactory());
            createdColumns.put(col, createdCol);
        }
        return createdColumns;
    }

    private ReferenceTable materializeAppendTable(final ReferenceTable table, final Progress progress,
        final ColumnarVirtualTable sourceTable, final ColumnarVirtualTable appendedColumns)
        throws CanceledExecutionException, VirtualTableIncompatibleException {
        var appendedColsWithOriginalRowID = sourceTable.filterColumns(0).append(List.of(appendedColumns));

        // the cursor contains the original rowID for progress reporting while the container has a void row key
        // since it is filtered out anyway when the table is appended to the input table
        var appendCursorSchema = appendedColsWithOriginalRowID.getSchema();
        final var executor =
            new GraphVirtualTableExecutor(appendedColsWithOriginalRowID.getVirtualTable().getProducingTransform());
        try (final var container = createAppendContainer(appendedColumns.getSchema(), m_tableIdSupplier.getAsInt());
                var writeCursor = container.createCursor();
                var mappedRows = executor.execute(table.getSources()).get(0);
                var readCursor = mappedRows.createCursor()) {
            var accessRow = readCursor.access();
            var rowRead = VirtualTableUtils.createRowRead(appendCursorSchema, accessRow,
                new DefaultColumnSelection(mappedRows.getSchema().numColumns()));
            for (long r = 0; readCursor.forward(); r++) {
                // exploits that VoidRowKeyWriteValue#setRowKey is a noop
                writeCursor.forward().setFrom(rowRead);
                progress.update(r, rowRead);
            }
            var appendTable = container.finish();
            return ReferenceTables.createReferenceTable(UUID.randomUUID(), appendTable);
        } catch (CanceledExecutionException canceledException) {
            // this stunt is necessary because ColumnarRowContainerUtils.create throws Exception
            throw canceledException;
        } catch (VirtualTableIncompatibleException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to create append table.", ex);
        }
    }

    private interface Progress {
        void update(final long rowIndex, final RowRead rowRead) throws CanceledExecutionException;
    }

    private abstract static class AbstractProgress implements Progress {
        protected final ExecutionMonitor m_monitor;

        protected final long m_size;

        AbstractProgress(final ExecutionMonitor monitor, final long size) {
            m_monitor = monitor;
            m_size = size;
        }

    }

    private static final class SimpleProgress extends AbstractProgress {

        SimpleProgress(final ExecutionMonitor monitor, final long size) {
            super(monitor, size);
        }

        @Override
        public void update(final long rowIndex, final RowRead rowRead) throws CanceledExecutionException {
            m_monitor.setProgress(rowIndex + 1 / ((double)m_size));
            m_monitor.checkCanceled();
        }

    }

    private static final class CellFactoryProgress extends AbstractProgress {
        private final CellFactory m_cellFactory;

        CellFactoryProgress(final ExecutionMonitor monitor, final long size, final CellFactory cellFactory) {
            super(monitor, size);
            m_cellFactory = cellFactory;
        }

        @Override
        public void update(final long rowIndex, final RowRead rowRead) throws CanceledExecutionException {
            m_cellFactory.setProgress(rowIndex + 1, m_size, new RowKey(rowRead.getRowKey().getString()), m_monitor);
            m_monitor.checkCanceled();
        }

    }

    private ConversionResult convertColumns(final ColumnarVirtualTable table,
        final List<RearrangedColumn> columnsToConvert) {
        var convertedTable = table;
        var convertedColumns = new LinkedHashMap<RearrangedColumn, ColumnarVirtualTable>();
        int numColumns = table.getSchema().numColumns();
        for (var columnToConvert : columnsToConvert) {
            // the MapBuilder has to be recreated in each iteration because the same column might be converted
            // multiple times. This doesn't make sense but is supported by the API, so we also have to implement
            // it to stay backwards compatible.
            var mapBuilder = new MapBuilder(convertedTable);
            int colIdxIncludingRowID = columnToConvert.getConverterIndex() + 1;
            var convertedColumn = mapBuilder.convert(columnToConvert.getConverter(), colIdxIncludingRowID);
            convertedColumns.put(columnToConvert, convertedColumn);
            var insertPermutation = IntStream.range(0, numColumns)//
                    .map(i -> i < colIdxIncludingRowID ? i : (i == colIdxIncludingRowID ? (numColumns - 1) : i - 1))
                    .toArray();
            var filter = IntStream.range(0, numColumns)//
                .filter(i -> i != colIdxIncludingRowID)//
                .toArray();
            convertedTable = convertedTable.filterColumns(filter)//
                .append(List.of(convertedColumn))//
                .permute(insertPermutation);
        }
        return new ConversionResult(convertedTable, convertedColumns);
    }

    private record ConversionResult(ColumnarVirtualTable tableWithConvertedColumns,
        Map<RearrangedColumn, ColumnarVirtualTable> convertedColumns) {
    }

    private RowContainer createAppendContainer(final ColumnarValueSchema schema, final int tableId) throws Exception {
        var schemaWithRowID = prependRowID(VoidRowKeyFactory.INSTANCE, schema);
        var dataContainerSettings = DataContainerSettings.getDefault();
        var columnarContainerSettings =
            new ColumnarRowWriteTableSettings(true, dataContainerSettings.getMaxDomainValues(), false, false, 100, 4);
        return ColumnarRowContainerUtils.create(m_context, tableId, schemaWithRowID, columnarContainerSettings);
    }

    private static ColumnarValueSchema prependRowID(final RowKeyValueFactory<?, ?> rowIDValueFactory,
        final ColumnarValueSchema schema) {
        assert !ColumnarValueSchemaUtils.hasRowID(schema) : "The ColumnarValueSchema already has a RowID";
        var valueFactories = Stream.concat(//
            Stream.of(rowIDValueFactory), //
            IntStream.range(0, schema.numColumns()).mapToObj(schema::getValueFactory)//
        ).toArray(ValueFactory<?, ?>[]::new);
        return ColumnarValueSchemaUtils.create(schema.getSourceSpec(), valueFactories);
    }

    private final class MapBuilder {

        private final GenericValueFactory[] m_inputValueFactories;

        // currently includes all columns
        private final int[] m_inputIndices;

        private final ColumnarVirtualTable m_sourceTable;

        MapBuilder(final ColumnarVirtualTable sourceTable) {
            m_sourceTable = sourceTable;
            var schema = m_sourceTable.getSchema();
            m_inputValueFactories = new GenericValueFactory[schema.numColumns()];
            Arrays.setAll(m_inputValueFactories, i -> new GenericValueFactory(schema.getValueFactory(i)));
            m_inputIndices = IntStream.range(0, m_inputValueFactories.length).toArray();
        }

        ColumnarVirtualTable convert(final DataCellTypeConverter converter, final int colIndexIncludingRowID) {
            var inputValueFactory = m_inputValueFactories[colIndexIncludingRowID];
            var outputType = converter.getOutputType();
            var outputValueFactory = getValueFactory(outputType);
            var colSpecCreator = new DataColumnSpecCreator(
                m_sourceTable.getSchema().getSourceSpec().getColumnSpec(colIndexIncludingRowID - 1));
            colSpecCreator.setType(outputType);
            var converterFactory =
                new ConverterFactory(converter, inputValueFactory, outputValueFactory, colSpecCreator.createSpec());
            return m_sourceTable.map(converterFactory, colIndexIncludingRowID);
        }

        ColumnarVirtualTable map(final CellFactory cellFactory) {
            var outputSpecs = cellFactory.getColumnSpecs();
            var valueFactories = Stream.of(outputSpecs)//
                .map(DataColumnSpec::getType)//
                .map(this::getValueFactory)//
                .toArray(GenericValueFactory[]::new);
            var cellFactoryMap = new CellFactoryMap(m_inputValueFactories, valueFactories, cellFactory);
            return m_sourceTable.map(cellFactoryMap, m_inputIndices);
        }

        private GenericValueFactory getValueFactory(final DataType type) {
            return new GenericValueFactory(ValueFactoryUtils.getValueFactory(type, m_fsHandler));
        }
    }

    private static final class ConverterFactory implements ColumnarMapperFactory {
        private final DataCellTypeConverter m_converter;

        private final ColumnarValueSchema m_outputSchema;

        private final GenericValueFactory m_inputValueFactory;

        private final GenericValueFactory m_outputValueFactory;

        ConverterFactory(final DataCellTypeConverter converter, final GenericValueFactory inputValueFactory,
            final GenericValueFactory outputValueFactory, final DataColumnSpec convertedSpec) {
            m_inputValueFactory = inputValueFactory;
            m_outputValueFactory = outputValueFactory;
            m_outputSchema = ColumnarValueSchemaUtils.create(new DataTableSpec(convertedSpec),
                new ValueFactory<?, ?>[]{outputValueFactory.m_valueFactory});
            m_converter = converter;
        }

        @Override
        public ColumnarValueSchema getOutputSchema() {
            return m_outputSchema;
        }

        @Override
        public Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var inputAccess = inputs[0];
            var inputValue = new NullableReadValue(m_inputValueFactory.createReadValue(inputAccess), inputAccess);
            var outputAccess = outputs[0];
            var outputValue = m_outputValueFactory.createWriteValue(outputAccess);
            return r -> {
                var convertedCell = m_converter.callConvert(inputValue.getDataCell());
                if (convertedCell.isMissing()) {
                    outputAccess.setMissing();
                } else {
                    unsafeSetValue(outputValue, convertedCell);
                }
            };
        }

        @SuppressWarnings("unchecked")
        private static <D extends DataValue> void unsafeSetValue(final WriteValue<D> writeValue,
            final DataValue value) {
            writeValue.setValue((D)value);
        }

    }

    private static class CellFactoryMap implements ColumnarMapperFactory {

        private final GenericValueFactory[] m_readValueFactories;

        private final GenericValueFactory[] m_writeValueFactories;

        private final CellFactory m_cellFactory;

        private ColumnarValueSchema m_schema;

        CellFactoryMap(final GenericValueFactory[] inputValueFactories,
            final GenericValueFactory[] outputValueFactories, final CellFactory cellFactory) {
            m_readValueFactories = inputValueFactories;
            m_writeValueFactories = outputValueFactories;
            m_cellFactory = cellFactory;
            m_schema = ColumnarValueSchemaUtils.create(new DataTableSpec(cellFactory.getColumnSpecs()), Stream
                .of(outputValueFactories).map(GenericValueFactory::getValueFactory).toArray(ValueFactory<?, ?>[]::new));
        }

        @Override
        public Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var rowKey = (RowKeyValue)m_readValueFactories[0].createReadValue(inputs[0]);
            var readValues = IntStream.range(1, inputs.length)//
                .mapToObj(i -> new NullableReadValue(m_readValueFactories[i].createReadValue(inputs[i]), inputs[i]))
                .toArray(NullableReadValue[]::new);
            var writeValues = new NullableWriteValue[outputs.length];
            Arrays.setAll(writeValues,
                i -> new NullableWriteValue<>(m_writeValueFactories[i].createWriteValue(outputs[i]), outputs[i]));
            return new CellFactoryMapper(rowKey, readValues, writeValues, m_cellFactory);
        }

        @Override
        public ColumnarValueSchema getOutputSchema() {
            return m_schema;
        }

    }

    private static final class CellFactoryMapper implements MapperFactory.Mapper {

        private final RowKeyValue m_rowKey;

        private final NullableReadValue[] m_readValues;

        private final NullableWriteValue<?>[] m_writeValues;

        private final CellFactory m_cellFactory;

        CellFactoryMapper(final RowKeyValue rowKey, final NullableReadValue[] readValues,
            final NullableWriteValue<?>[] writeValues, final CellFactory cellFactory) {
            m_rowKey = rowKey;
            m_readValues = readValues;
            m_writeValues = writeValues;
            m_cellFactory = cellFactory;
        }

        @Override
        public void map(final long rowIndex) {
            var rowKey = new RowKey(m_rowKey.getString());
            var cells = Stream.of(m_readValues)//
                .map(NullableReadValue::getDataCell)//
                .toArray(DataCell[]::new);
            var row = new DefaultRow(rowKey, cells);
            var outputs = m_cellFactory.getCells(row, rowIndex);
            IntStream.range(0, m_writeValues.length)//
                .forEach(i -> m_writeValues[i].setDataCell(outputs[i]));
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

        ValueFactory<?, ?> getValueFactory() {
            return m_valueFactory;
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
