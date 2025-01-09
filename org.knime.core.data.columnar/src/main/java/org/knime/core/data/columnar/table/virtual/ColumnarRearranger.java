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
import java.util.List;
import java.util.Optional;
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
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperFactory;
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
            .sorted(Comparator.comparing(RearrangedColumn::getOriginalIndex)).toList();

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

        return new VirtualTableExtensionTable(refTables.toArray(ReferenceTable[]::new), rearrangedTable, table.size(),
            m_tableIdSupplier.getAsInt());

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
     * @param inputTable the input table
     * @param newColumns columns that need to be created
     * @param monitor for progress updates
     * @param size of the input table
     * @return a new table consisting of the materialized new columns
     * @throws VirtualTableIncompatibleException
     * @throws CanceledExecutionException
     */
    private ReferenceTable createAppendTable(final ReferenceTable inputTable, final List<RearrangedColumn> newColumns,
        final ExecutionMonitor monitor, final long size)
        throws VirtualTableIncompatibleException, CanceledExecutionException {
        var table = inputTable.getVirtualTable();

        // the column indices we will select for materializing after replacing/appending new columns.
        final int[] selection = new int[newColumns.size() + 1];
        int nextSelectionIndex = 0;

        // always select the rowKey column
        selection[nextSelectionIndex] = 0;
        nextSelectionIndex++;

        // --------------------------------------------------------------------
        // columns produced by DataCellTypeConverters
        var columnsToConvert = newColumns.stream()//
            .filter(RearrangedColumn::isConvertedColumn)//
            .toList();
        // conversion has to happen before the CellFactories are applied
        if (!columnsToConvert.isEmpty()) {
            for (var columnToConvert : columnsToConvert) {
                final int colIndex = columnToConvert.getConverterIndex() + 1;
                table = replaceConvert(table, columnToConvert.getConverter(), colIndex);
                selection[nextSelectionIndex] = colIndex;
                nextSelectionIndex++;
            }
        }

        // --------------------------------------------------------------------
        // columns produced by CellFactories
        var columnsToCreate = newColumns.stream()//
            .filter(RearrangedColumn::isNewColumn)//
            .toList();
        if (!columnsToCreate.isEmpty()) {
            table = createColumns(newColumns, table, selection, nextSelectionIndex);
        }

        // --------------------------------------------------------------------
        // materialize
        var uniqueCellFactories = columnsToCreate.stream()//
            .map(RearrangedColumn::getCellFactory)//
            .collect(Collectors.toSet());

        final Progress progress;
        if (columnsToCreate.isEmpty()) {
            progress = new SimpleProgress(monitor, size);
        } else {
            // TODO (AP-23838) Is it desired to only set progress on the column 0 CellFactory?
            progress = new CellFactoryProgress(monitor, size, columnsToCreate.get(0).getCellFactory());
        }

        ColumnRearrangerUtils.initProcessing(uniqueCellFactories, m_context);
        try {
            var appendTable = table.selectColumns(selection);
            return ColumnarVirtualTableMaterializer.materializer() //
                .sources(inputTable.getSources()) //
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
     * <p>
     * Writes indices of appended columns into the given {@code int[] selection} array, starting at
     * {@code selection[nextSelectionIndex]}. The columns are appended to selection in the order they appear in
     * {@code columnsToCreate}. (This may be different than the order of appended columns in the result table.)
     *
     * @param columnsToCreate the new columns to create
     * @param table input table with potentially converted columns
     * @param selection column indices of columns to materialize. New columns created by this method will be appended
     *            starting at {@code nextSelectionIndex}.
     * @param nextSelectionIndex where to start appending column indices of {@code columnsToCreate} to {@code selection}
     * @return a {@link ColumnarVirtualTable} with new columns appended
     */
    private ColumnarVirtualTable createColumns(final List<RearrangedColumn> columnsToCreate, ColumnarVirtualTable table,
        final int[] selection, int nextSelectionIndex) {

        int numColumns = table.getSchema().numColumns();
        final int[] inputColumnIndices = IntStream.range(0, numColumns).toArray();
        final ValueFactory<?, ?>[] inputValueFactories = inputValueFactories(table);

        // maps CellFactory to the index of the first appended column created by that CellFactory
        final var cellFactoryToFirstColumnIndex = new HashMap<CellFactory, Integer>();

        for (var col : columnsToCreate) {
            // one cell factory can create multiple columns,
            // therefore only the first column per CellFactory creates the ColumnarVirtualTable containing all of its columns
            final CellFactory cellFactory = col.getCellFactory();
            final Integer existingStartIndex = cellFactoryToFirstColumnIndex.putIfAbsent(cellFactory, numColumns);
            final int startIndex = Optional.ofNullable(existingStartIndex).orElse(numColumns);
            if (existingStartIndex == null) {
                table = appendMap(table, cellFactory, inputColumnIndices, inputValueFactories);
                numColumns += cellFactory.getColumnSpecs().length;
            }
            selection[nextSelectionIndex] = col.getIndexInFactory() + startIndex;
            nextSelectionIndex++;
        }
        return table;
    }

    private static ValueFactory<?, ?>[] inputValueFactories(final ColumnarVirtualTable table) {
        final var schema = table.getSchema();
        final var factories = new ValueFactory[schema.numColumns()];
        Arrays.setAll(factories, schema::getValueFactory);
        return factories;
    }

    /**
     * Append columns created by the the given {@code cellFactory} to the input {@code table}.
     *
     * @param table input table
     * @param cellFactory for creating new columns
     * @param inputIndices columns in {@code table} to provide to the {@code cellFactory}
     * @param inputValueFactories ValueFactories for the input columns
     * @return a ColumnarVirtualTable that appends columns created by {@code cellFactory} to {@code table}
     */
    private ColumnarVirtualTable appendMap(final ColumnarVirtualTable table, final CellFactory cellFactory,
        final int[] inputIndices, final ValueFactory<?, ?>[] inputValueFactories) {
        var outputSpecs = cellFactory.getColumnSpecs();
        var valueFactories = Stream.of(outputSpecs)//
            .map(DataColumnSpec::getType)//
            .map(this::getValueFactory)//
            .toArray(ValueFactory[]::new);
        var cellFactoryMap = new CellFactoryMap(inputValueFactories, valueFactories, cellFactory);
        return table.appendMap(cellFactoryMap, inputIndices);
    }

    private ValueFactory<?, ?> getValueFactory(final DataType type) {
        return ValueFactoryUtils.getValueFactory(type, m_fsHandler);
    }

    /**
     * Creates a ColumnarVirtualTable where the given column is replaced using {@code converter}.
     *
     * @param table input table
     * @param converter converter to use for conversion
     * @param colIndexIncludingRowID index of the column to be converted
     * @return
     */
    private ColumnarVirtualTable replaceConvert(final ColumnarVirtualTable table, final DataCellTypeConverter converter,
        final int colIndexIncludingRowID) {
        final ValueSchema schema = table.getSchema();
        final DataTableSpec sourceSpec = schema.getSourceSpec();
        final DataType outputType = converter.getOutputType();
        final ValueFactory<?, ?> inputValueFactory = schema.getValueFactory(colIndexIncludingRowID);
        final ValueFactory<?, ?> outputValueFactory = ValueFactoryUtils.getValueFactory(outputType, m_fsHandler);
        final var colSpecCreator = new DataColumnSpecCreator(sourceSpec.getColumnSpec(colIndexIncludingRowID - 1));
        colSpecCreator.setType(outputType);
        final DataColumnSpec outputColSpec = colSpecCreator.createSpec();
        final var converterFactory =
            new ConverterFactory(converter, inputValueFactory, outputValueFactory, outputColSpec);
        return table.replaceMap(converterFactory, colIndexIncludingRowID);
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

    private static class ConverterFactory implements ColumnarMapperFactory {
        private final DataCellTypeConverter m_converter;

        private final ValueSchema m_outputSchema;

        private final ValueFactory<?, ?> m_inputValueFactory;

        private final ValueFactory<?, ?> m_outputValueFactory;

        ConverterFactory(final DataCellTypeConverter converter, final ValueFactory<?, ?> inputValueFactory,
            final ValueFactory<?, ?> outputValueFactory, final DataColumnSpec convertedSpec) {
            m_inputValueFactory = inputValueFactory;
            m_outputValueFactory = outputValueFactory;
            m_outputSchema = ValueSchemaUtils.create(new DataTableSpec(convertedSpec),
                new ValueFactory<?, ?>[]{m_outputValueFactory});
            m_converter = converter;
        }

        @Override
        public ValueSchema getOutputSchema() {
            return m_outputSchema;
        }

        @Override
        public Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var inputAccess = inputs[0];
            var inputValue = createNullableReadValue(m_inputValueFactory, inputAccess);
            var outputAccess = outputs[0];
            var outputValue = createNullableWriteValue(m_outputValueFactory, outputAccess);
            return () -> outputValue.setDataCell(m_converter.callConvert(inputValue.getDataCell()));
        }

    }

    private static class CellFactoryMap implements ColumnarMapperWithRowIndexFactory {

        private final ValueFactory<?, ?>[] m_readValueFactories;

        private final ValueFactory<?, ?>[] m_writeValueFactories;

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
