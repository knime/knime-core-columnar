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
 *   Feb 27, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataColumnSpecCreator.MergeOptions;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;

/**
 * Contains the logic for concatenating tables in a KNIME way.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarConcatenater {

    /**
     * The options {@link DataColumnSpecCreator#merge(DataColumnSpec, java.util.Set)} is called with.
     */
    private static final EnumSet<MergeOptions> MERGE_OPTIONS =
        EnumSet.of(MergeOptions.ALLOW_VARYING_TYPES, MergeOptions.ALLOW_VARYING_ELEMENT_NAMES);

    private final ExecutionMonitor m_progress;

    private final IntSupplier m_tableIdSupplier;

    private final boolean m_checkForDuplicateIDs;

    /**
     * Constructor.
     *
     * @param tableIdSupplier supplies IDs for new tables
     * @param progress for updating the progress
     * @param checkForDuplicateIDs whether RowIDs should be checked for duplicates
     */
    public ColumnarConcatenater(final IntSupplier tableIdSupplier, final ExecutionMonitor progress,
        final boolean checkForDuplicateIDs) {
        m_progress = progress;
        m_tableIdSupplier = tableIdSupplier;
        m_checkForDuplicateIDs = checkForDuplicateIDs;
    }

    /**
     * @param tables to concatenate
     * @return the concatenated table
     * @throws VirtualTableIncompatibleException if the tables can't be efficiently concatenated with fast tables
     */
    public VirtualTableExtensionTable concatenate(final BufferedDataTable[] tables)
        throws VirtualTableIncompatibleException {
        var concatenatedSchema = concatenate(VirtualTableSchemaUtils.extractSchemas(tables));
        final long concatenatedSize = Stream.of(tables).mapToLong(BufferedDataTable::size).sum();
        if (m_checkForDuplicateIDs) {
            checkForDuplicateIDs(m_progress, tables, concatenatedSize);
        }
        var refTables = ReferenceTables.createReferenceTables(tables);
        var virtualTableFragment = createVirtualTableFragment(refTables, concatenatedSchema);
        return new VirtualTableExtensionTable(refTables, virtualTableFragment, concatenatedSchema, concatenatedSize,
            m_tableIdSupplier.getAsInt());
    }

    private static ColumnarValueSchema concatenate(final ColumnarValueSchema[] schemas) {
        final var tableSpecs = VirtualTableSchemaUtils.extractSpecs(schemas);
        var unionSchemaCreators = new LinkedHashMap<String, ColCreator>();
        for (int t = 0; t < schemas.length; t++) {
            var schema = schemas[t];
            var spec = tableSpecs[t];
            for (int c = 0; c < spec.getNumColumns(); c++) {
                var colSpec = spec.getColumnSpec(c);
                // +1 because RowIDs are not part of DataTableSpec
                var valueFactory = schema.getValueFactory(c + 1);
                unionSchemaCreators.computeIfAbsent(colSpec.getName(), n -> new ColCreator(colSpec, valueFactory))//
                    .merge(colSpec, valueFactory);
            }
        }
        var unionTableSpec = new DataTableSpec(
            unionSchemaCreators.values().stream().map(ColCreator::createSpec).toArray(DataColumnSpec[]::new));
        var valueFactories = Stream.concat(//
            Stream.of(schemas[0].getValueFactory(0)), // Assumes that all tables use the same value factory for RowIDs
            unionSchemaCreators.values().stream().map(ColCreator::getValueFactory)).toArray(ValueFactory[]::new);
        return ColumnarValueSchemaUtils.create(ValueSchemaUtils.create(unionTableSpec, valueFactories));
    }

    private static VirtualTable createVirtualTableFragment(final ReferenceTable[] tables,
        final ColumnarValueSchema concatenatedSchema) {
        var tablePrepper = new TablePrepper(concatenatedSchema);
        var first = tablePrepper.prepare(tables[0]);
        return first.concatenate(Stream.of(tables).skip(1).map(tablePrepper::prepare).collect(toList()));
    }

    private static final class TablePrepper {

        private final Map<String, ValueFactory<?, ?>> m_valueFactoriesPerColumn;

        TablePrepper(final ColumnarValueSchema concatenatedSchema) {
            var tableSpec = concatenatedSchema.getSourceSpec();
            m_valueFactoriesPerColumn = new HashMap<>();
            for (int c = 0; c < tableSpec.getNumColumns(); c++) {
                // +1 because DataTableSpec doesn't have the RowID as dedicated column
                m_valueFactoriesPerColumn.put(tableSpec.getColumnSpec(c).getName(),
                    concatenatedSchema.getValueFactory(c + 1));
            }
        }

        VirtualTable prepare(final ReferenceTable table) {
            var schema = table.getSchema();
            var spec = schema.getSourceSpec();
            var colMapperFactories = new LinkedHashMap<Integer, ValueFactoryMapperFactory<?>>();
            for (int c = 0; c < spec.getNumColumns(); c++) {
                var concatenateValueFactory = m_valueFactoriesPerColumn.get(spec.getColumnSpec(c).getName());
                ValueFactory<ReadAccess, WriteAccess> valueFactory = schema.getValueFactory(c + 1);
                if (!concatenateValueFactory.getClass().equals(valueFactory.getClass())) {
                    colMapperFactories.put(c + 1,
                        new ValueFactoryMapperFactory<>(valueFactory, concatenateValueFactory));
                }
            }
            // we start a new fragment here
            var virtualTable = new VirtualTable(table.getId(), table.getSchema());
            if (!colMapperFactories.isEmpty()) {
                var mapperFactory = new ValueFactoriesMapperFactory(
                    colMapperFactories.values().toArray(ValueFactoryMapperFactory[]::new));
                var indices = colMapperFactories.keySet().stream().mapToInt(Integer::intValue).toArray();
                virtualTable = virtualTable.map(indices, mapperFactory);
            }
            return virtualTable;
        }
    }

    private static final class ValueFactoriesMapperFactory implements MapperFactory {

        private final ValueFactoryMapperFactory<?>[] m_mapperFactories;

        private final ColumnarSchema m_schema;

        ValueFactoriesMapperFactory(final ValueFactoryMapperFactory<?>[] mapperFactories) {
            m_mapperFactories = mapperFactories;
            var specs = Stream.of(mapperFactories)//
                .map(ValueFactoryMapperFactory::outputValueFactory)//
                .map(ValueFactory::getSpec)//
                .toArray(DataSpec[]::new);
            var traits = Stream.of(mapperFactories)//
                .map(ValueFactoryMapperFactory::outputValueFactory)//
                .map(ValueFactory::getTraits)//
                .toArray(DataTraits[]::new);
            m_schema = new DefaultColumnarSchema(specs, traits);
        }

        @Override
        public ColumnarSchema getOutputSchema() {
            return m_schema;
        }

        @Override
        public Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var mappers = IntStream.range(0, m_mapperFactories.length)//
                .mapToObj(i -> m_mapperFactories[i].createMapper(inputs[i], outputs[i]))//
                .toArray(Runnable[]::new);
            return () -> Stream.of(mappers).forEach(Runnable::run);
        }

    }

    @SuppressWarnings("hiding")
    private record ValueFactoryMapperFactory<R extends ReadValue> (ValueFactory<?, ?> inputValueFactory,
        ValueFactory<?, ?> outputValueFactory) {

        Runnable createMapper(final ReadAccess input, final WriteAccess output) {
            var readValue = createReadValue(inputValueFactory, input);
            var writeValue = createWriteValue(outputValueFactory, output);
            return () -> writeValue.setValue(readValue);
        }

        @SuppressWarnings("unchecked")
        private final <A extends ReadAccess> R createReadValue(final ValueFactory<A, ?> valueFactory,
            final ReadAccess access) {
            return (R)valueFactory.createReadValue((A)access);
        }

        @SuppressWarnings("unchecked")
        private final <A extends WriteAccess> WriteValue<R> createWriteValue(final ValueFactory<?, A> valueFactory,
            final WriteAccess access) {
            return (WriteValue<R>)valueFactory.createWriteValue((A)access);
        }
    }

    private static final class ColCreator {

        private final DataColumnSpecCreator m_specCreator;

        private ValueFactory<?, ?> m_valueFactory;

        ColCreator(final DataColumnSpec colSpec, final ValueFactory<?, ?> valueFactory) {
            m_specCreator = new DataColumnSpecCreator(colSpec);
            m_valueFactory = valueFactory;
        }

        void merge(final DataColumnSpec colSpec, final ValueFactory<?, ?> valueFactory) {
            if (colSpec.getType().isASuperTypeOf(m_specCreator.getType())) {
                m_valueFactory = valueFactory;
            }
            m_specCreator.merge(colSpec, MERGE_OPTIONS);
        }

        DataColumnSpec createSpec() {
            return m_specCreator.createSpec();
        }

        ValueFactory<?, ?> getValueFactory() {
            return m_valueFactory;
        }
    }

    private static void checkForDuplicateIDs(final ExecutionMonitor progMon, final BufferedDataTable[] tables,
        final long concatenatedSize) {
        final var checker = new DuplicateChecker();
        final var progress = new Progress(progMon, concatenatedSize);
        try {
            checkForDuplicateKeys(progress, checker, tables);
        } finally {
            checker.clear();
        }
    }

    private static void checkForDuplicateKeys(final Progress progress, final DuplicateChecker checker,
        final BufferedDataTable[] tables) {
        for (var i = 0; i < tables.length; i++) {
            try {
                addKeys(progress, checker, tables[i]);
            } catch (DuplicateKeyException ex) {
                throw new IllegalArgumentException("Duplicate row key \"" + ex.getKey() + "\" in table with index " + i,
                    ex);
            } catch (IOException ex) {
                throw new IllegalArgumentException("An I/O problem occurred while checking for duplicate keys.", ex);
            }
        }
        try {
            checker.checkForDuplicates();
        } catch (DuplicateKeyException | IOException ex) {
            throw new IllegalArgumentException("Duplicate row keys", ex);
        }
    }

    private static void addKeys(final Progress progress, final DuplicateChecker checker, final BufferedDataTable table)
        throws DuplicateKeyException, IOException {
        try (RowCursor cursor = table.cursor(TableFilter.materializeCols())) {
            while (cursor.canForward()) {
                final var key = cursor.forward().getRowKey().getString();
                checker.addKey(key);
                progress.report(key);
            }
        }
    }

    private static final class Progress {

        private final ExecutionMonitor m_progressMonitor;

        private final long m_totalSize;

        private final double m_doubleSize;

        private long m_currentRow = 0;

        Progress(final ExecutionMonitor progressMonitor, final long totalSize) {
            m_progressMonitor = progressMonitor;
            m_totalSize = totalSize;
            m_doubleSize = totalSize;
        }

        void report(final String key) {
            m_progressMonitor.setProgress(m_currentRow / m_doubleSize,
                () -> String.format("Checking tables, row %d/%d ('%s')", m_currentRow, m_totalSize, key));
        }

    }

}
