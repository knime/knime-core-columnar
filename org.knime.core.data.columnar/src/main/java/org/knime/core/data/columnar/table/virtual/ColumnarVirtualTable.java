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

import static org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils.hasRowID;
import static org.knime.core.table.virtual.spec.SelectColumnsTransformSpec.indicesAfterDrop;
import static org.knime.core.table.virtual.spec.SelectColumnsTransformSpec.indicesAfterKeepOnly;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperWithRowIndexFactory;
import org.knime.core.table.virtual.spec.MaterializeTransformSpec;
import org.knime.core.table.virtual.spec.ProgressListenerTransformSpec.ProgressListenerFactory;
import org.knime.core.table.virtual.spec.ProgressListenerTransformSpec.ProgressListenerWithRowIndexFactory;
import org.knime.core.table.virtual.spec.ProgressListenerTransformSpec.ProgressListenerWithRowIndexFactory.ProgressListener;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.google.common.collect.Collections2;

/**
 * Equivalent to {@link VirtualTable} with the difference that {@link #getSchema()} returns a
 * {@link ColumnarValueSchema}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarVirtualTable {

    private final TableTransform m_transform;

    private final ColumnarValueSchema m_valueSchema;

    /**
     * Constructs a ColumnarVirtualTable around a source table.
     *
     * @param sourceIdentifier ID of the source
     * @param schema of the source
     * @param lookahead flag indicating whether the source is capable of lookahead
     */
    public ColumnarVirtualTable(final UUID sourceIdentifier, final ColumnarValueSchema schema,
        final boolean lookahead) {
        m_transform =
            new TableTransform(new SourceTransformSpec(sourceIdentifier, new SourceTableProperties(schema, lookahead)));
        m_valueSchema = schema;
    }

    /**
     * Constructs a ColumnarVirtualTable from a transform and the schema of the output of the transform.
     *
     * @param transform of the table
     * @param schema of the table
     */
    public ColumnarVirtualTable(final TableTransform transform,final ColumnarValueSchema schema) {
        m_transform = transform;
        m_valueSchema = schema;
    }

    /**
     * @return the schema of the table
     */
    public ColumnarValueSchema getSchema() {
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
        final ColumnarValueSchema schema = selectColumns(m_valueSchema, columnIndices);
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), schema);
    }

    private static ColumnarValueSchema selectColumns(final ColumnarValueSchema schema, final int... columnIndices) {
        var hasRowID = hasRowID(schema);
        if (hasRowID && Arrays.stream(columnIndices).skip(1).anyMatch(i -> i == 0)) {
            // If schema has a RowID, then the RowID column (in this table) is at index 0.
            // It must either be dropped or remain at index 0.
            throw new IllegalArgumentException("RowID must either be dropped or remain at index 0");
        }
        var valueFactories = IntStream.of(columnIndices)//
                .mapToObj(schema::getValueFactory)//
                .toArray(ValueFactory<?, ?>[]::new);
        var originalSpec = schema.getSourceSpec();
        var specCreator = new DataTableSpecCreator(originalSpec);
        specCreator.dropAllColumns();
        var permutationStream = IntStream.of(columnIndices);
        if (hasRowID) {
            // The RowID is not part of the DataTableSpec.
            permutationStream = permutationStream
                    .filter(i -> i > 0) // skip if present
                    .map(i -> i - 1); // translate "indices including RowID" to "indices without RowID"
        }
        specCreator.addColumns(permutationStream.mapToObj(originalSpec::getColumnSpec).toArray(DataColumnSpec[]::new));
        return createColumnarValueSchema(valueFactories, specCreator.createSpec());
    }

    /**
     * Drops columns in the table.
     *
     * @param columnIndices of the columns to drop
     * @return the table without the dropped columns
     */
    ColumnarVirtualTable dropColumns(final int... columnIndices) {
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
     * Appends the provided table to this table.
     *
     * @param table to append
     * @return the appended table
     */
    public ColumnarVirtualTable append(final ColumnarVirtualTable table) {
        return append(List.of(table));
    }

    /**
     * If the appended {@code tables} have RowID columns, these are dropped.
     * Only the RowID of {@code this} table is kept (if present).
     */
    ColumnarVirtualTable append(final List<ColumnarVirtualTable> tables) {
        var tablesWithoutRowIDs = tables.stream()//
            .map(ColumnarVirtualTable::filterRowID)//
            .toList();
        var transformSpec = new AppendTransformSpec();
        var schema = appendSchemas(collectSchemas(tablesWithoutRowIDs));
        var transforms = collectTransforms(tablesWithoutRowIDs);
        return new ColumnarVirtualTable(new TableTransform(transforms, transformSpec), schema);
    }

    ColumnarVirtualTable replaceSchema(final ColumnarValueSchema schema) {
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
     * A {@link MapperFactory} whose {@link #getOutputSchema()} method returns a {@link ColumnarValueSchema}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface ColumnarMapperFactory extends MapperFactory {
        @Override
        ColumnarValueSchema getOutputSchema();
    }

    /**
     * A {@link MapperWithRowIndexFactory} whose {@link #getOutputSchema()} method returns a {@link ColumnarValueSchema}.
     */
    public interface ColumnarMapperWithRowIndexFactory extends MapperWithRowIndexFactory {
        @Override
        ColumnarValueSchema getOutputSchema();
    }

    ColumnarVirtualTable concatenate(final List<ColumnarVirtualTable> tables) {
        CheckUtils.checkArgument(
            tables.stream().map(ColumnarVirtualTable::getSchema).allMatch(this::isConcatenateCompatible),
            "The schemas are not compatible for concatenation, i.e. don't have the same types and column names.");
        var incomingTransforms = collectTransforms(tables);
        return new ColumnarVirtualTable(new TableTransform(incomingTransforms, new ConcatenateTransformSpec()),
            m_valueSchema);
    }
    private boolean isConcatenateCompatible(final ColumnarValueSchema schema) {
        return m_valueSchema.numColumns() == schema.numColumns()//
            && m_valueSchema.getSourceSpec().equalStructure(schema.getSourceSpec())//
            && IntStream.range(0, m_valueSchema.numColumns())
                .allMatch(i -> ValueFactoryUtils.areEqual(m_valueSchema.getValueFactory(i), schema.getValueFactory(i)));
    }

    private static ColumnarVirtualTable filterRowID(final ColumnarVirtualTable table) {
        return hasRowID(table.getSchema()) ? table.dropColumns(0) : table;
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

    private List<TableTransform> collectTransforms(final List<ColumnarVirtualTable> tables) {
        final List<TableTransform> transforms = new ArrayList<>(1 + tables.size());
        transforms.add(m_transform);
        transforms.addAll(Collections2.transform(tables, ColumnarVirtualTable::getProducingTransform));
        return transforms;
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



    ColumnarVirtualTable materialize(final UUID sinkIdentifier) {
        final MaterializeTransformSpec transformSpec = new MaterializeTransformSpec(sinkIdentifier);
        var emptySchema = createColumnarValueSchema(new ValueFactory<?, ?>[0], new DataTableSpec());
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), emptySchema);
    }


    // TODO (TP) Implement RowIndex propagation.
    //      (*) MapperWithRowIndexFactory should probably get the actual row index from a Source node?
    //      (*) How to handle sliced Sources? Should indices start at 0 or at slice.from?
    //      (*) Add a signature
    //          VirtualTable.map(int[], MapperWithRowIndexFactory, VirtualTable),
    //          where the VirtualTable argument specifies which VirtualTable the row index should be taken from.
    //          Then the method below would be equivalent to
    //          VirtualTable.map(int[] c, MapperWithRowIndexFactory f) {
    //              return map(c,f,this);
    //          }
    ColumnarVirtualTable map(final ColumnarMapperWithRowIndexFactory mapperFactory, final int... columnIndices) {
        final TableTransformSpec transformSpec = new MapTransformSpec(columnIndices, mapperFactory);
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), mapperFactory.getOutputSchema());
    }


    // TODO (TP) Implement ProgressTransformSpec handling.
    //      As a workaround, we use a RowFilter that always evaluates to {@code
    //      true} but this should be fixed, because it stands in the way of
    //      optimizations, destroys lookahead capability for no reason, etc...
    // TODO (TP) rename to "observe()"? (ObserverFactory, ObserverWithRowIndexFactory, etc?)
    private ColumnarVirtualTable progress(final ProgressListenerFactory factory, final int... columnIndices) {
        final TableTransformSpec transformSpec = new RowFilterTransformSpec(columnIndices, wrapAsRowFilterFactory(factory));
        return new ColumnarVirtualTable(new TableTransform(m_transform, transformSpec), m_valueSchema);
    }

    private static RowFilterFactory wrapAsRowFilterFactory(final ProgressListenerFactory factory) {
        return inputs -> {
            Runnable progress = factory.createProgressListener(inputs);
            return () -> {
                progress.run();
                return true;
            };
        };
    }

    /**
     * Adds a progress (observer) to the table.
     *
     * @param factory for the progress listener
     * @param columnIndices indices the progress listener observes
     * @return table containing the progress listener
     */
    ColumnarVirtualTable progress(final ProgressListenerWithRowIndexFactory factory, final int... columnIndices) {
        return progress(wrapAsProgressListenerFactory(factory), columnIndices);
    }

    private static ProgressListenerFactory
        wrapAsProgressListenerFactory(final ProgressListenerWithRowIndexFactory factory) {
        return new ProgressListenerFactory() {
            @Override
            public Runnable createProgressListener(final ReadAccess[] inputs) {
                ProgressListener progress = factory.createProgressListener(inputs);
                return new Runnable() {
                    private long m_rowIndex = 0;

                    @Override
                    public void run() {
                        progress.update(m_rowIndex);
                        m_rowIndex++;
                    }
                };
            }
        };
    }

}
