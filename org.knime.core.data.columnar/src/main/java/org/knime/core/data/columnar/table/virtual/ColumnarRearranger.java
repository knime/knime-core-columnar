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

import static org.knime.core.data.columnar.table.virtual.NullableValues.createNullableReadValue;
import static org.knime.core.data.columnar.table.virtual.NullableValues.createNullableWriteValue;
import static org.knime.core.data.columnar.table.virtual.NullableValues.createReadValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataCellTypeConverter;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperWithRowIndexFactory;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTableMaterializer.Progress;
import org.knime.core.data.columnar.table.virtual.NullableValues.NullableReadValue;
import org.knime.core.data.columnar.table.virtual.NullableValues.NullableWriteValue;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.ColumnRearrangerUtils;
import org.knime.core.data.container.ColumnRearrangerUtils.RearrangedColumn;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.Node;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.virtual.spec.MapTransformUtils.MapperWithRowIndexFactory;

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

        // separate new and existing columns for further processing
        var newColumns = rearrangedColumns.stream()//
            .filter(ColumnarRearranger::isAppendedColumn)//
            .toList();
        var existingColumns = rearrangedColumns.stream()//
            .filter(Predicate.not(ColumnarRearranger::isAppendedColumn))//
            // ensure that the existing columns are in the order they are in the input table
            .sorted(Comparator.comparing(RearrangedColumn::getOriginalIndex))
            .toList();

        // keep only RowID and included columns in their original order
        var tableToRearrange = asFragmentSource(refTable).selectColumns(//
            addRowIDIndex(//
                existingColumns.stream()//
                    .mapToInt(RearrangedColumn::getOriginalIndex)//
            ).toArray()//
        );

        // tableToRearrange := input table filtered to keep only (RowID and) columns that are used un-converted.
        // keeps the same order of columns as in input table
        // tableToRearrange := tableToRearrange with the newly created columns (converted and mapped) appended
        // then pass that to rearrange(...)
        if (!newColumns.isEmpty()) {
            var appendTable = createAppendTable(refTable, newColumns, progressMonitor, table.size());
            refTables.add(appendTable);
            var appendTableSource = asFragmentSource(appendTable).dropColumns(0); // filter out the void row key column
            tableToRearrange = tableToRearrange.append(appendTableSource);
        }

        var rearrangedTable = rearrange(newColumns, existingColumns, tableToRearrange);

        // the RearrangeColumnsTable removes the table name and table properties, so we do the same
        rearrangedTable = dropOldSpecNameAndProperties(rearrangedTable);

        return new VirtualTableExtensionTable(refTables.toArray(ReferenceTable[]::new),
            rearrangedTable, table.size(), m_tableIdSupplier.getAsInt());

    }

    private static ColumnarVirtualTable dropOldSpecNameAndProperties(final ColumnarVirtualTable rearrangedTable) {
        var rearrangedSchema = rearrangedTable.getSchema();
        var rearrangedSpec = rearrangedSchema.getSourceSpec();
        var columns = rearrangedSpec.stream().toArray(DataColumnSpec[]::new);
        return rearrangedTable
            .replaceSchema(ValueSchemaUtils.updateDataTableSpec(rearrangedSchema, new DataTableSpec(columns)));
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
        return tableToRearrange.selectColumns(permutation);
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

    /**
     * Creates a new table consisting of the new columns. New columns are either produced by
     * {@link DataCellTypeConverter converters} or by {@link CellFactory cellFactories}.
     *
     * @param table the unfiltered input table
     * @param newColumns columns that need to be created
     * @param monitor for progress updates
     * @param size of the input table
     * @return a new table consisting of the materialized new columns
     * @throws VirtualTableIncompatibleException
     * @throws CanceledExecutionException
     */
    private ReferenceTable createAppendTable(final ReferenceTable table, final List<RearrangedColumn> newColumns,
        final ExecutionMonitor monitor, final long size)
        throws VirtualTableIncompatibleException, CanceledExecutionException {
        var sourceTable = table.getVirtualTable();

        // columns produced by DataCellTypeConverters
        var columnsToConvert = newColumns.stream()//
            .filter(RearrangedColumn::isConvertedColumn)//
            .toList();
        var mappedColumns = new LinkedHashMap<RearrangedColumn, ColumnarVirtualTable>();
        // conversion has to happen before the CellFactories are applied
        if (!columnsToConvert.isEmpty()) {
            var conversionResult = convertColumns(sourceTable, columnsToConvert);
            sourceTable = conversionResult.tableWithConvertedColumns();
            mappedColumns.putAll(conversionResult.convertedColumns());
        }

        // columns produced by CellFactories
        var columnsToCreate = newColumns.stream()//
            .filter(RearrangedColumn::isNewColumn)//
            .toList();

        Progress progress;
        if (columnsToCreate.isEmpty()) {
            progress = new SimpleProgress(monitor, size);
        } else {
            mappedColumns.putAll(createColumns(columnsToCreate, sourceTable));
            progress = new CellFactoryProgress(monitor, size, columnsToCreate.get(0).getCellFactory());
        }

        var mappedColumnList = mappedColumns.entrySet().stream()//
            .sorted(Comparator.comparingInt(e -> e.getKey().getNewColIndex()))
            .map(Map.Entry::getValue)//
            .toList();

        ColumnarVirtualTable appendedColumns =
            mappedColumnList.get(0).append(mappedColumnList.subList(1, mappedColumnList.size()));

        var uniqueCellFactories = columnsToCreate.stream()//
                .map(RearrangedColumn::getCellFactory)//
                .collect(Collectors.toSet());

        ColumnRearrangerUtils.initProcessing(uniqueCellFactories, m_context);
        try {
            var appendTable = sourceTable.selectColumns(0).append(appendedColumns); // use RowID from sourceTable
            return ColumnarVirtualTableMaterializer.materializer() //
                .sources(table.getSources()) //
                .materializeRowKey(false) //
                .progress(progress) //
                .executionContext(m_context) //
                .tableIdSupplier(m_tableIdSupplier) //
                .materialize(appendTable);
        } finally {
            ColumnRearrangerUtils.finishProcessing(uniqueCellFactories);
        }
    }

    /**
     * Sets up the creation of new columns via CellFactories.
     *
     * @param columnsToCreate the new columns to create
     * @param table input table with potentially converted columns
     * @return a map from the RearrangedColumns to their respective ColumnarVirtualTable
     */
    private Map<RearrangedColumn, ColumnarVirtualTable> createColumns(final List<RearrangedColumn> columnsToCreate,
        final ColumnarVirtualTable table) {
        var mapBuilder = new MapBuilder(table);
        var cellFactoryTables = new HashMap<CellFactory, ColumnarVirtualTable>();
        var createdColumns = new LinkedHashMap<RearrangedColumn, ColumnarVirtualTable>();
        for (var col : columnsToCreate) {
            // one cell factory can create multiple columns, therefore only the first column per CellFactory creates
            // the ColumnarVirtualTable containing all of its columns
            var cellFactoryTable = cellFactoryTables.computeIfAbsent(col.getCellFactory(), mapBuilder::map);
            // select the current column from the cellFactoryTable to establish the mapping from RearrangeColumn
            // to their ColumnarVirtualTable slice
            var createdCol = cellFactoryTable.selectColumns(col.getIndexInFactory());
            createdColumns.put(col, createdCol);
        }
        return createdColumns;
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
        public void update(final long rowIndex, final RowKeyValue rowKey) {
            m_monitor.setProgress(rowIndex / ((double)m_size));
            try {
                m_monitor.checkCanceled();
            } catch (CanceledExecutionException ex) {
                throw new CompletionException(ex);
            }
        }
    }

    private static final class CellFactoryProgress extends AbstractProgress {
        private final CellFactory m_cellFactory;

        CellFactoryProgress(final ExecutionMonitor monitor, final long size, final CellFactory cellFactory) {
            super(monitor, size);
            m_cellFactory = cellFactory;
        }

        @Override
        public void update(final long rowIndex, final RowKeyValue rowKey) {
            m_cellFactory.setProgress(rowIndex, m_size, new RowKey(rowKey.getString()), m_monitor);
            try {
                m_monitor.checkCanceled();
            } catch (CanceledExecutionException ex) {
                throw new CompletionException(ex);
            }
        }
    }

    /**
     * Converts the columnsToConvert in the input table.
     *
     * @param table containing the columns to convert
     * @param columnsToConvert the columns to convert
     * @return ConversionResult consisting of the converted table and a map of the individual converted columns
     */
    private ConversionResult convertColumns(final ColumnarVirtualTable table,
        final List<RearrangedColumn> columnsToConvert) {
        var convertedTable = table;
        var convertedColumns = new LinkedHashMap<RearrangedColumn, ColumnarVirtualTable>();
        int numColumns = table.getSchema().numColumns();
        var mapBuilder = new MapBuilder(convertedTable);
        for (var columnToConvert : columnsToConvert) {
            int colIdxIncludingRowID = columnToConvert.getConverterIndex() + 1;
            var convertedColumn = mapBuilder.convert(columnToConvert.getConverter(), colIdxIncludingRowID);
            convertedColumns.put(columnToConvert, convertedColumn);

            var insertPermutation = IntStream.range(0, numColumns)//
                    .map(i -> {
                        if (i < colIdxIncludingRowID) {
                            return i;
                        } else if (i == colIdxIncludingRowID) {
                            return numColumns - 1;
                        } else {
                            return i - 1;
                        }
                    }).toArray();
            convertedTable = convertedTable.dropColumns(colIdxIncludingRowID)//
                .append(convertedColumn)//
                .selectColumns(insertPermutation);
        }
        return new ConversionResult(convertedTable, convertedColumns);
    }

    private record ConversionResult(ColumnarVirtualTable tableWithConvertedColumns,
        Map<RearrangedColumn, ColumnarVirtualTable> convertedColumns) {
    }

    /**
     * Helper class for creating new columns via
     * {@link ColumnarVirtualTable#map(ColumnarMapperWithRowIndexFactory, int...)}.
     * Always assumes the presence of a RowID column.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private final class MapBuilder {

        private final ValueFactory<?, ?>[] m_inputValueFactories;

        // currently includes all columns
        private final int[] m_inputIndices;

        private final ColumnarVirtualTable m_sourceTable;

        MapBuilder(final ColumnarVirtualTable sourceTable) {
            m_sourceTable = sourceTable;
            var schema = m_sourceTable.getSchema();
            m_inputValueFactories = new ValueFactory[schema.numColumns()];
            Arrays.setAll(m_inputValueFactories, i -> schema.getValueFactory(i));
            m_inputIndices = IntStream.range(0, m_inputValueFactories.length).toArray();
        }

        /**
         * Creates a single column ColumnarVirtualTable containing the column converted by converter.
         *
         * @param converter to use for conversion
         * @param colIndexIncludingRowID the index of the column. The first column has index 1 because of the RowID
         * @return a single column ColumnarVirtualTable defining the converted column
         */
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

        /**
         * Creates a ColumnarVirtualTable by applying the CellFactory. The returned table has as many columns as the
         * CellFactory produces.
         *
         * @param cellFactory for creating new columns
         * @return the ColumnarVirtualTable defining the creation of columns via the provided CellFactory
         */
        ColumnarVirtualTable map(final CellFactory cellFactory) {
            var outputSpecs = cellFactory.getColumnSpecs();
            var valueFactories = Stream.of(outputSpecs)//
                .map(DataColumnSpec::getType)//
                .map(this::getValueFactory)//
                .toArray(ValueFactory[]::new);
            var cellFactoryMap = new CellFactoryMap(m_inputValueFactories, valueFactories, cellFactory);
            return m_sourceTable.map(cellFactoryMap, m_inputIndices);
        }

        private ValueFactory<?, ?> getValueFactory(final DataType type) {
            return ValueFactoryUtils.getValueFactory(type, m_fsHandler);
        }
    }

    private static final class ConverterFactory implements ColumnarMapperWithRowIndexFactory {
        private final DataCellTypeConverter m_converter;

        private final ValueSchema m_outputSchema;

        private final ValueFactory<?, ?> m_inputValueFactory;

        private final ValueFactory<?, ?> m_outputValueFactory;

        ConverterFactory(final DataCellTypeConverter converter, final ValueFactory<?, ?> inputValueFactory,
            final ValueFactory<?, ?> outputValueFactory, final DataColumnSpec convertedSpec) {
            m_inputValueFactory = inputValueFactory;
            m_outputValueFactory = outputValueFactory;
            m_outputSchema = ValueSchemaUtils.create(new DataTableSpec(convertedSpec), outputValueFactory);
            m_converter = converter;
        }

        @Override
        public ValueSchema getOutputSchema() {
            return m_outputSchema;
        }

        @Override
        public Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var inputAccess = inputs[0];
            var inputValue = createNullableReadValue(m_inputValueFactory, inputAccess);
            var outputAccess = outputs[0];
            var outputValue = createNullableWriteValue(m_outputValueFactory, outputAccess);
            return r -> outputValue.setDataCell(m_converter.callConvert(inputValue.getDataCell()));
        }

    }

    private static class CellFactoryMap implements ColumnarMapperWithRowIndexFactory {

        private final ValueFactory<?,?>[] m_readValueFactories;

        private final ValueFactory<?,?>[] m_writeValueFactories;

        private final CellFactory m_cellFactory;

        private final ValueSchema m_schema;

        CellFactoryMap(final ValueFactory<?, ?>[] inputValueFactories, final ValueFactory<?, ?>[] outputValueFactories,
            final CellFactory cellFactory) {
            m_readValueFactories = inputValueFactories;
            m_writeValueFactories = outputValueFactories;
            m_cellFactory = cellFactory;
            m_schema = ValueSchemaUtils.create(//
                new DataTableSpec(cellFactory.getColumnSpecs()), //
                outputValueFactories);
        }

        @Override
        public Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            RowKeyValue rowKey = createReadValue(m_readValueFactories[0], inputs[0]);
            var readValues = IntStream.range(1, inputs.length)//
                .mapToObj(i -> createNullableReadValue(m_readValueFactories[i], inputs[i]))
                .toArray(NullableReadValue[]::new);
            var writeValues = new NullableWriteValue[outputs.length];
            Arrays.setAll(writeValues, i -> createNullableWriteValue(m_writeValueFactories[i], outputs[i]));
            return new CellFactoryMapper(rowKey, readValues, writeValues, m_cellFactory);
        }

        @Override
        public ValueSchema getOutputSchema() {
            return m_schema;
        }

    }

    private static final class CellFactoryMapper implements MapperWithRowIndexFactory.Mapper {

        private final NullableWriteValue[] m_writeValues;

        private final CellFactory m_cellFactory;

        private final ReadValueRow m_row;

        CellFactoryMapper(final RowKeyValue rowKey, final NullableReadValue[] readValues,
            final NullableWriteValue[] writeValues, final CellFactory cellFactory) {
            m_row = new ReadValueRow(rowKey, readValues);
            m_writeValues = writeValues;
            m_cellFactory = cellFactory;
        }

        @Override
        public void map(final long rowIndex) {
            var outputs = m_cellFactory.getCells(m_row, rowIndex);
            IntStream.range(0, m_writeValues.length)//
                .forEach(i -> m_writeValues[i].setDataCell(outputs[i]));
        }

    }

    private static final class ReadValueRow implements DataRow {

        private final RowKeyValue m_rowID;

        private final NullableReadValue[] m_columns;

        ReadValueRow(final RowKeyValue rowID, final NullableReadValue[] columns) {
            m_rowID = rowID;
            m_columns = columns;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return Stream.of(m_columns)//
                    .map(NullableReadValue::getDataCell)//
                    .iterator();
        }

        @Override
        public int getNumCells() {
            return m_columns.length;
        }

        @Override
        public RowKey getKey() {
            return new RowKey(m_rowID.getString());
        }

        @Override
        public DataCell getCell(final int index) {
            return m_columns[index].getDataCell();
        }


    }

}
