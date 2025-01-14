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

import static org.knime.core.table.virtual.spec.SelectColumnsTransformSpec.indicesAfterDrop;
import static org.knime.core.table.virtual.spec.SelectColumnsTransformSpec.indicesAfterKeepOnly;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataColumnSpecCreator.MergeOptions;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.DataTableValueSchemaUtils;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchema.ValueSchemaColumn;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.data.v2.value.LongValueFactory;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.access.LongAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.spec.AppendMapTransformSpec;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.MapTransformUtils.MapperWithRowIndexFactory;
import org.knime.core.table.virtual.spec.MapTransformUtils.MapperWithRowIndexFactory.Mapper;
import org.knime.core.table.virtual.spec.ObserverTransformSpec;
import org.knime.core.table.virtual.spec.ObserverTransformSpec.ObserverFactory;
import org.knime.core.table.virtual.spec.ObserverTransformUtils;
import org.knime.core.table.virtual.spec.ObserverTransformUtils.ObserverWithRowIndexFactory;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.google.common.collect.Collections2;

/**
 * Equivalent to {@link VirtualTable} with the difference that {@link #getSchema()} returns a {@link ValueSchema}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarVirtualTable {

    private final TableTransform m_transform;

    private final ValueSchema m_valueSchema;

    /**
     * Constructs a ColumnarVirtualTable around a source table.
     *
     * @param sourceIdentifier ID of the source
     * @param schema of the source
     * @param lookahead flag indicating whether the source is capable of lookahead
     */
    public ColumnarVirtualTable(final UUID sourceIdentifier, final ValueSchema schema,
        final boolean lookahead) {
        this(sourceIdentifier, schema, lookahead ? CursorType.LOOKAHEAD : CursorType.BASIC);
    }

    /**
     * Constructs a ColumnarVirtualTable around a source table.
     *
     * @param sourceIdentifier ID of the source
     * @param schema of the source
     * @param cursorType which Cursor types the source provides ({@link Cursor}, {@link LookaheadCursor}, or
     *            {@link RandomAccessCursor})
     */
    public ColumnarVirtualTable(final UUID sourceIdentifier, final ValueSchema schema,
        final CursorType cursorType) {
        m_transform = new TableTransform(
            new SourceTransformSpec(sourceIdentifier, new SourceTableProperties(schema, cursorType)));
        m_valueSchema = schema;
    }

    /**
     * Constructs a ColumnarVirtualTable from a transform and the schema of the output of the transform.
     *
     * @param transform of the table
     * @param schema of the table
     */
    public ColumnarVirtualTable(final TableTransform transform, final ValueSchema schema) {
        m_transform = transform;
        m_valueSchema = schema;
    }

    /**
     * @return the schema of the table
     */
    public ValueSchema getSchema() {
        return m_valueSchema;
    }

    /**
     * @return the transform that creates the table
     */
    public TableTransform getProducingTransform() {
        return m_transform;
    }

    /**
     * Selects columns from the table in a particular order.
     *
     * @param columnIndices indices of the columns to select
     * @return the table consisting of the selected columns in the order they were selected
     */
    public ColumnarVirtualTable selectColumns(final int... columnIndices) {
        final TableTransformSpec transformSpec = new SelectColumnsTransformSpec(columnIndices);
        final ValueSchema schema = ValueSchemaUtils.selectColumns(m_valueSchema, columnIndices);
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), schema);
    }

    /**
     * Drops columns in the table.
     *
     * @param columnIndices of the columns to drop
     * @return the table without the dropped columns
     */
    public ColumnarVirtualTable dropColumns(final int... columnIndices) {
        return selectColumns(indicesAfterDrop(m_valueSchema.numColumns(), columnIndices));
    }

    /**
     * Keeps only the specified columns.
     *
     * @param columnIndices of the columns to keep
     * @return the table with only the kept columns
     */
    ColumnarVirtualTable keepOnlyColumns(final int... columnIndices) {
        return selectColumns(indicesAfterKeepOnly(columnIndices));
    }

    /**
     * Slices the table along the row dimension
     *
     * @param from index of the first row to include (inclusive)
     * @param to end index of the slice (exclusive)
     * @return the sliced table consisting only of the rows in [from, to)
     */
    public ColumnarVirtualTable slice(final long from, final long to) {
        final TableTransformSpec transformSpec = new SliceTransformSpec(from, to);
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), m_valueSchema);
    }

    /**
     * Appends the provided tables to this table.
     *
     * @param tables to append
     * @return the appended table
     */
    public ColumnarVirtualTable append(final List<ColumnarVirtualTable> tables) {
        var transformSpec = new AppendTransformSpec();
        var transforms = collectTransforms(tables);
        var schema = appendSchemas(collectSchemas(tables));
        return new ColumnarVirtualTable(new TableTransform(transforms, transformSpec), schema);
    }

    /**
     * Appends the provided tables to this table.
     *
     * @param tables to append
     * @return the appended table
     */
    public ColumnarVirtualTable append(final ColumnarVirtualTable... tables) {
        return append(Arrays.asList(tables));
    }

    /**
     * Create a {@code new ColumnarVirtualTable} by including only rows from this {@code
     * ColumnarVirtualTable} that match a given predicate. This is defined by an array of {@code n} column indices that
     * form the inputs of the ({@code n}-ary} filter predicate. The predicate is evaluated on the values of the
     * respective columns for each row. Rows for which the predicate evaluates to {@code true} will be included, rows
     * for which the filter predicate evaluates to {@code false} will be removed (skipped). The filter is given by a
     * {@code RowFilterFactory} which can be used to create multiple instances of the filter predicate for processing
     * multiple lines in parallel. (Each filter predicate is used single-threaded.) The order in which
     * {@code columnIndices} are given matters. For example if {@code columnIndices = {5,1,4}}, then values from the
     * 5th, 1st, and 4th column are provided as inputs 0, 1, and 2, respectively, to the filter predicate.
     *
     * @param columnIndices the indices of the columns that are passed to the filter predicate
     * @param filterFactory factory to create instances of the filter predicate
     * @return the filtered table
     */
    public ColumnarVirtualTable filterRows(final int[] columnIndices, final RowFilterFactory filterFactory) {
        final TableTransformSpec transformSpec = new RowFilterTransformSpec(columnIndices, filterFactory);
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), m_valueSchema);
    }

    public ColumnarVirtualTable appendMissingValueColumns(final ValueSchema schema) {
        var transformSpec = new AppendMissingValuesTransformSpec(schema);
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec),
            appendSchemas(m_valueSchema, schema));
    }

    ColumnarVirtualTable replaceSchema(final ValueSchema schema) {
        CheckUtils.checkArgument(schema.numColumns() == m_valueSchema.numColumns(), "The number of columns must match");
        for (int i = 0; i < schema.numColumns(); i++) {//NOSONAR
            var currentValueFactory = m_valueSchema.getValueFactory(i);
            var newValueFactory = schema.getValueFactory(i);
            CheckUtils.checkArgument(ValueFactoryUtils.areEqual(currentValueFactory, newValueFactory),
                "The ValueFactories in column %s don't match.", i);
        }
        return new ColumnarVirtualTable(m_transform, schema);
    }

    /**
     * A {@link MapperFactory} whose {@link #getOutputSchema()} method returns a {@link ValueSchema}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface ColumnarMapperFactory extends MapperFactory {
        @Override
        ValueSchema getOutputSchema();
    }

    /**
     * A {@link MapperWithRowIndexFactory} whose {@link #getOutputSchema()} method returns a {@link ValueSchema}.
     */
    public interface ColumnarMapperWithRowIndexFactory extends MapperWithRowIndexFactory {
        @Override
        ValueSchema getOutputSchema();
    }

    /**
     * A {@code ColumnarMapperFactory} implementation that wraps a {@code ColumnarMapperWithRowIndexFactory}.
     * <p>
     * Mappers created by this factory have one additional {@code LongReadAccess} input wrt mappers created by the
     * wrapped {@code MapperWithRowIndexFactory}. This additional input represents the row index. When the mapper is
     * {@code run()}, this input is stripped off and passed as the rowIndex argument to the wrapped
     * {@link Mapper#map(long) Mapper.map(long)}.
     */
    public static class WrappedColumnarMapperWithRowIndexFactory implements ColumnarMapperFactory {

        private final ColumnarMapperWithRowIndexFactory factory;

        /**
         * Wrap the given {@code MapperWithRowIndexFactory} as a simple {@code
         * MapperFactory} with the row index appended as an additional input {@code LongReadAccess}.
         */
        public WrappedColumnarMapperWithRowIndexFactory(final ColumnarMapperWithRowIndexFactory factory) {
            this.factory = factory;
        }

        @Override
        public ValueSchema getOutputSchema() {
            return factory.getOutputSchema();
        }

        @Override
        public Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {

            // the last input is the rowIndex
            final LongAccess.LongReadAccess rowIndex = (LongAccess.LongReadAccess)inputs[inputs.length - 1];

            // create a MapperWithRowIndex with the remaining inputs
            final ReadAccess[] inputsWithoutRowIndex = Arrays.copyOf(inputs, inputs.length - 1);
            final MapperWithRowIndexFactory.Mapper mapper = factory.createMapper(inputsWithoutRowIndex, outputs);

            return () -> mapper.map(rowIndex.getLongValue());
        }

        public ColumnarMapperWithRowIndexFactory getMapperWithRowIndexFactory() {
            return factory;
        }
    }

    public ColumnarVirtualTable concatenate(final ColumnarVirtualTable table) {
        return concatenate(List.of(table));
    }

    public ColumnarVirtualTable concatenate(final List<ColumnarVirtualTable> tables) {
        CheckUtils.checkArgument(tables.stream()//
            .map(ColumnarVirtualTable::getSchema)//
            .allMatch(this::isConcatenateCompatible), //
            "The schemas are not compatible for concatenation, i.e. don't have the same types and column names.");
        var incomingTransforms = collectTransforms(tables);

        return new ColumnarVirtualTable(new TableTransform(incomingTransforms, new ConcatenateTransformSpec()),
            concatenateDomainAndMetaData(
                tables.stream().map(ColumnarVirtualTable::getSchema).toArray(ValueSchema[]::new)));
    }

    private ValueSchema concatenateDomainAndMetaData(final ValueSchema... schemas) {
        final var columns = new ValueSchemaColumn[m_valueSchema.numColumns()];
        Arrays.setAll(columns, i -> concatenateDomainAndMetaData(//
            m_valueSchema.getColumn(i), //
            Arrays.stream(schemas).map(schema -> schema.getColumn(i))));
        return ValueSchemaUtils.create(columns);
    }

    /**
     * The options {@link DataColumnSpecCreator#merge(DataColumnSpec, java.util.Set)} is called with. This is the same
     * as in AppendedRowsTable
     */
    private static final EnumSet<MergeOptions> MERGE_OPTIONS =
        EnumSet.of(MergeOptions.ALLOW_VARYING_TYPES, MergeOptions.ALLOW_VARYING_ELEMENT_NAMES);

    private static ValueSchemaColumn concatenateDomainAndMetaData(final ValueSchemaColumn column,
        final Stream<ValueSchemaColumn> columns) {
        if (column.dataColumnSpec() == null) {
            return column;
        }
        final var specCreator = new DataColumnSpecCreator(column.dataColumnSpec());
        columns.map(ValueSchemaColumn::dataColumnSpec).forEach(colSpec -> specCreator.merge(colSpec, MERGE_OPTIONS));
        final var spec = specCreator.createSpec();
        return new ValueSchemaColumn(spec, column.valueFactory());
    }

    private boolean isConcatenateCompatible(final ValueSchema schema) {
        return m_valueSchema.numColumns() == schema.numColumns()//
            && IntStream.range(0, m_valueSchema.numColumns())
                .allMatch(i -> m_valueSchema.getColumn(i).equalStructure(schema.getColumn(i)));
    }

    private static ValueSchema appendSchemas(final ValueSchema... schemas) {
        return appendSchemas(Arrays.asList(schemas));
    }

    private static ValueSchema appendSchemas(final List<ValueSchema> schemas) {
        var columns = schemas.stream()//
            .flatMap(schema -> Stream.of(schema.getColumns()))//
            .toList();
        return ValueSchemaUtils.create(columns);
    }

    private static ValueSchema appendRowIndex(final ValueSchema schema, final String columnName) {
        var columns = Arrays.copyOf(schema.getColumns(), schema.numColumns() + 1);
        columns[columns.length - 1] = new ValueSchemaColumn(columnName, LongCell.TYPE, LongValueFactory.INSTANCE);
        return ValueSchemaUtils.create(columns);
    }

    private static String tmpUniqueRowIndexColumnName() {
        return "row_idx-" + UUID.randomUUID().toString();
    }

    private List<TableTransform> collectTransforms(final List<ColumnarVirtualTable> tables) {
        final List<TableTransform> transforms = new ArrayList<>(1 + tables.size());
        transforms.add(m_transform);
        transforms.addAll(Collections2.transform(tables, ColumnarVirtualTable::getProducingTransform));
        return transforms;
    }

    private List<ValueSchema> collectSchemas(final List<ColumnarVirtualTable> tables) {
        return Stream.concat(//
            Stream.of(m_valueSchema), //
            tables.stream().map(ColumnarVirtualTable::getSchema))//
            .collect(Collectors.toList());
    }

    /**
     * Append a LONG column that contains the current row index.
     *
     * @param columnName the name of the row index column
     */
    public ColumnarVirtualTable appendRowIndex(final String columnName) {
        final RowIndexTransformSpec transformSpec = new RowIndexTransformSpec();
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec),
            appendRowIndex(m_valueSchema, columnName));
    }

    /**
     * Return a new {@code ColumnarVirtualTable} comprising (only) the columns produced by {@code mapperFactory}. The
     * mapper returned by the {@code mapperFactory} doesn't have to be thread-safe. (For multi-threaded computation, the
     * {@code mapperFactory} will be asked to produce a mapper for every thread.)
     *
     * @param mapperFactory produces a mapper that is run for every row in this virtual table.
     * @param columnIndices indices of columns used by the mapper.
     * @return a new table comprising the columns produced by the mapper
     */
    public ColumnarVirtualTable map(final ColumnarMapperFactory mapperFactory, final int... columnIndices) {
        final TableTransformSpec transformSpec = new MapTransformSpec(columnIndices, mapperFactory);
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec),
            mapperFactory.getOutputSchema());
    }

    /**
     * Return a new {@code ColumnarVirtualTable} comprising (only) the columns produced by {@code mapperFactory}. The
     * mapper returned by the {@code mapperFactory} doesn't have to be thread-safe. (For multi-threaded computation, the
     * {@code mapperFactory} will be asked to produce a mapper for every thread.)
     *
     * @param mapperFactory produces a mapper that is run for every row in this virtual table.
     * @param columnIndices indices of columns used by the mapper.
     * @return a new table comprising the columns produced by the mapper
     */
    public ColumnarVirtualTable map(final ColumnarMapperWithRowIndexFactory mapperFactory, final int... columnIndices) {
        final int[] columns = Arrays.copyOf(columnIndices, columnIndices.length + 1);
        columns[columns.length - 1] = m_valueSchema.numColumns();
        final ColumnarMapperFactory factory = new WrappedColumnarMapperWithRowIndexFactory(mapperFactory);
        return appendRowIndex(tmpUniqueRowIndexColumnName()).map(factory, columns);
    }

    /**
     * Return a new {@code ColumnarVirtualTable} comprising the columns of this table and the columns produced by
     * {@code mapperFactory}. The mapper returned by the {@code mapperFactory} doesn't have to be thread-safe. (For
     * multi-threaded computation, the {@code mapperFactory} will be asked to produce a mapper for every thread.)
     *
     * @param mapperFactory produces a mapper that is run for every row in this table.
     * @param columnIndices indices of columns used by the mapper.
     * @return a new table appending the columns produced by the mapper to this table
     */
    public ColumnarVirtualTable appendMap(final ColumnarMapperFactory mapperFactory, final int... columnIndices) {
        final TableTransformSpec transformSpec = new AppendMapTransformSpec(columnIndices, mapperFactory);
        final ValueSchema schema = appendSchemas(m_valueSchema, mapperFactory.getOutputSchema());
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), schema);
    }

    /**
     * Return a new {@code ColumnarVirtualTable} comprising the columns of this table and the columns produced by
     * {@code mapperFactory}. The mapper returned by the {@code mapperFactory} doesn't have to be thread-safe. (For
     * multi-threaded computation, the {@code mapperFactory} will be asked to produce a mapper for every thread.)
     *
     * @param mapperFactory produces a mapper that is run for every row in this table.
     * @param columnIndices indices of columns used by the mapper.
     * @return a new table appending the columns produced by the mapper to this table
     */
    public ColumnarVirtualTable appendMap(final ColumnarMapperWithRowIndexFactory mapperFactory, final int... columnIndices) {
        final int[] columns = Arrays.copyOf(columnIndices, columnIndices.length + 1);
        final int rowIndexColumn = m_valueSchema.numColumns();
        columns[columns.length - 1] = rowIndexColumn;
        final ColumnarMapperFactory factory = new WrappedColumnarMapperWithRowIndexFactory(mapperFactory);
        return appendRowIndex(tmpUniqueRowIndexColumnName()) //
            .appendMap(factory, columns) //
            .dropColumns(rowIndexColumn);
    }

    /**
     * Return a new {@code ColumnarVirtualTable} where the column at {@code columnIndex} is replaced with the column
     * produced by {@code mapperFactory}. The mapper returned by the {@code mapperFactory} doesn't have to be
     * thread-safe. (For multi-threaded computation, the {@code mapperFactory} will be asked to produce a mapper for
     * every thread.) The mapper must have exactly one input and exactly one output column.
     *
     * @param mapperFactory produces a mapper that is run for every row in this table.
     * @param columnIndices indices of columns used by the mapper.
     * @return a new table with the specified columns replaced by the mapper column
     */
    public ColumnarVirtualTable replaceMap(final ColumnarMapperFactory mapperFactory, final int columnIndex) {
        if (mapperFactory.getOutputSchema().numColumns() != 1) {
            throw new IllegalArgumentException("ColumnarMapperFactory must produce exactly 1 column");
        }
        final int[] selection = IntStream.range(0, m_valueSchema.numColumns()).toArray();
        selection[columnIndex] = selection.length;
        return appendMap(mapperFactory, columnIndex).selectColumns(selection);
    }

    /**
     * Return a new {@code ColumnarVirtualTable} where the column at {@code columnIndex} is replaced with the column
     * produced by {@code mapperFactory}. The mapper returned by the {@code mapperFactory} doesn't have to be
     * thread-safe. (For multi-threaded computation, the {@code mapperFactory} will be asked to produce a mapper for
     * every thread.) The mapper must have exactly one input and exactly one output column.
     *
     * @param mapperFactory produces a mapper that is run for every row in this table.
     * @param columnIndices indices of columns used by the mapper.
     * @return a new table with the specified columns replaced by the mapper column
     */
    public ColumnarVirtualTable replaceMap(final ColumnarMapperWithRowIndexFactory mapperFactory,
        final int columnIndex) {
        if (mapperFactory.getOutputSchema().numColumns() != 1) {
            throw new IllegalArgumentException("ColumnarMapperFactory must produce exactly 1 column");
        }
        final int[] selection = IntStream.range(0, m_valueSchema.numColumns()).toArray();
        selection[columnIndex] = selection.length;
        return appendMap(mapperFactory, columnIndex).selectColumns(selection);

    }

    public ColumnarVirtualTable observe(final ObserverFactory observerFactory, final int... columnIndices) {
        final ObserverTransformSpec transformSpec = new ObserverTransformSpec(columnIndices, observerFactory);
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), m_valueSchema);
    }

    /**
     * Adds a (progress) observer to the table.
     *
     * @param factory for the observer
     * @param columnIndices columns that will be passed to the observer
     * @return table containing the observer
     */
    public ColumnarVirtualTable observe(final ObserverWithRowIndexFactory observerFactory, final int... columnIndices) {
        final int[] columns = Arrays.copyOf(columnIndices, columnIndices.length + 1);
        final int rowIndexColumn = m_valueSchema.numColumns();
        columns[columns.length - 1] = rowIndexColumn;
        final ObserverFactory factory = new ObserverTransformUtils.WrappedObserverWithRowIndexFactory(observerFactory);
        return appendRowIndex(tmpUniqueRowIndexColumnName()) //
            .observe(factory, columns) //
            .dropColumns(rowIndexColumn);
    }

    /**
     * Create a new table, where the {@link #getSchema() ValueSchema} has {@code DataColumnSpec}s replaced by those of
     * the given {@code outputSpec}.
     * <p>
     * In the schema of this table, {@code columns[0]} must be the RowKey. The {@code DataType} of the columns of the
     * schema must correspond to those of the {@code outputSpec} (except {@code columns[0]}, the RowKey, which is not
     * part of {@code DataTableSpec}).
     *
     * @param outputSpec provides the DataColumnSpecs
     * @return new table with the updated schema
     * @throws IllegalArgumentException if the provided schema does not have RowKey at column 0 or DataTypes don't match
     */
    public ColumnarVirtualTable updateSchema(final DataTableSpec outputSpec) {
        final int numCols = m_valueSchema.numColumns();
        if (numCols < 1 || !m_valueSchema.getColumn(0).isRowKey()) {
            throw new IllegalArgumentException("expected schema with RowKey at column 0");
        }
        final int numDataCols = numCols - 1;
        if (outputSpec.getNumColumns() != numDataCols) {
            throw new IllegalArgumentException("schema has " + numDataCols
                + " data columns, but spec has " + outputSpec.getNumColumns());
        }
        for (int i = 0; i < numDataCols; ++i) {
            final DataType colTypeSpec = outputSpec.getColumnSpec(i).getType();
            final DataType colTypeSchema = m_valueSchema.getDataColumnSpec(i + 1).getType();
            if (!colTypeSpec.equals(colTypeSchema)) {
                throw new IllegalArgumentException("column type mismatch at column " + i + ": schema has "
                    + colTypeSchema + ", spec has " + colTypeSpec);
            }
        }
        final var valueFactories = new ValueFactory<?, ?>[numCols];
        Arrays.setAll(valueFactories, m_valueSchema::getValueFactory);
        final var updatedSchema = DataTableValueSchemaUtils.create(outputSpec, valueFactories);
        return new ColumnarVirtualTable(m_transform, updatedSchema);
    }
}
