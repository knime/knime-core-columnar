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
package org.knime.core.data.columnar.table.virtual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataColumnSpecCreator.MergeOptions;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.columnar.ColumnarTableBackend;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.VirtualTableSchemaUtils;
import org.knime.core.data.columnar.table.virtual.ColumnarSpecReplacer.ColumnCast;
import org.knime.core.data.columnar.table.virtual.TableCasterFactory.CastOperation;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.data.v2.value.VoidRowKeyFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.Node;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;

/**
 * Contains the logic for concatenating tables in a KNIME way.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarConcatenater {

    private static final ColumnarTableBackend BACKEND = new ColumnarTableBackend();

    /**
     * The options {@link DataColumnSpecCreator#merge(DataColumnSpec, java.util.Set)} is called with.
     */
    private static final EnumSet<MergeOptions> MERGE_OPTIONS =
        EnumSet.of(MergeOptions.ALLOW_VARYING_TYPES, MergeOptions.ALLOW_VARYING_ELEMENT_NAMES);

    private final ExecutionContext m_exec;

    private final IntSupplier m_tableIdSupplier;

    private final boolean m_checkForDuplicateIDs;

    private final String m_rowIDSuffix;

    private final IWriteFileStoreHandler m_fsHandler;

    /**
     * Constructor.
     *
     * @param tableIdSupplier supplies IDs for new tables
     * @param exec for creating tables
     * @param checkForDuplicateIDs whether RowIDs should be checked for duplicates
     * @param rowIDSuffix used for uniquifying RowIDs
     */
    public ColumnarConcatenater(final IntSupplier tableIdSupplier, final ExecutionContext exec,
        final boolean checkForDuplicateIDs, final String rowIDSuffix) {
        m_exec = exec;
        m_tableIdSupplier = tableIdSupplier;
        m_checkForDuplicateIDs = checkForDuplicateIDs;
        m_rowIDSuffix = rowIDSuffix;
        m_fsHandler = Node.invokeGetFileStoreHandler(exec);
    }

    /**
     * @param tables to concatenate
     * @param progressMonitor for providing progress
     * @return the concatenated table
     * @throws VirtualTableIncompatibleException if the tables can't be efficiently concatenated with fast tables
     */
    public VirtualTableExtensionTable concatenate(final BufferedDataTable[] tables,
        final ExecutionMonitor progressMonitor) throws VirtualTableIncompatibleException {
        var concatenatedSchema = concatenate(VirtualTableSchemaUtils.extractSchemas(tables));
        final long concatenatedSize = Stream.of(tables).mapToLong(BufferedDataTable::size).sum();
        if (m_checkForDuplicateIDs) {
            checkForDuplicateIDs(progressMonitor, tables, concatenatedSize);
        }
        var refTables = ReferenceTables.createReferenceTables(tables);

        var tablePrepper = new TablePrepper(concatenatedSchema);
        progressMonitor.setMessage("Prepare first table");
        var first = tablePrepper.prepare(refTables[0], progressMonitor.createSubProgress(1.0 / tables.length));
        List<ColumnarVirtualTable> preparedTables = new ArrayList<>(tables.length - 1);
        for (int i = 1; i < refTables.length; i++) {
            progressMonitor.setMessage("Prepare table nr %s.".formatted(i + 1));
            preparedTables
                .add(tablePrepper.prepare(refTables[i], progressMonitor.createSubProgress(1.0 / tables.length)));
        }
        var virtualTable = first.concatenate(preparedTables);

        return new VirtualTableExtensionTable(tablePrepper.getReferenceTables(), virtualTable, concatenatedSize,
            m_tableIdSupplier.getAsInt());
    }

    private ColumnarValueSchema concatenate(final ColumnarValueSchema[] schemas) {
        final var tableSpecs = VirtualTableSchemaUtils.extractSpecs(schemas);
        var unionSchemaCreators = new LinkedHashMap<String, ColCreator>();
        for (int t = 0; t < schemas.length; t++) {
            var schema = schemas[t];
            var spec = tableSpecs[t];
            for (int c = 0; c < spec.getNumColumns(); c++) {
                var colSpec = spec.getColumnSpec(c);
                // +1 because RowIDs are not part of DataTableSpec
                var valueFactory = schema.getValueFactory(c + 1);
                unionSchemaCreators
                    .computeIfAbsent(colSpec.getName(), n -> new ColCreator(colSpec, valueFactory))//
                    .merge(colSpec);
            }
        }
        var unionTableSpec = new DataTableSpec(
            unionSchemaCreators.values().stream().map(ColCreator::createSpec).toArray(DataColumnSpec[]::new));
        var valueFactories = Stream.concat(//
            Stream.of(schemas[0].getValueFactory(0)), // Assumes that all tables use the same value factory for RowIDs
            unionSchemaCreators.values().stream().map(ColCreator::getValueFactory)).toArray(ValueFactory[]::new);
        return ColumnarValueSchemaUtils.create(ValueSchemaUtils.create(unionTableSpec, valueFactories));
    }

    private final class RowIDUniquifier {

        private final Set<String> m_ids = new HashSet<>();

        private static void consumeRowIDs(final BufferedDataTable table, final Consumer<String> idConsumer,
            final ExecutionMonitor progress) {
            double size = table.size();
            try (var cursor = table.cursor(TableFilter.materializeCols())) {
                for (long r = 1; cursor.canForward(); r++) {
                    idConsumer.accept(cursor.forward().getRowKey().getString());
                    progress.setProgress(r / size);
                }
            }
        }

        BufferedDataTable uniquifyRowIDs(final BufferedDataTable table, final ExecutionMonitor progress) {
            if (m_ids.isEmpty()) {
                progress.setMessage("Read RowIDs of first table.");
                consumeRowIDs(table, m_ids::add, progress);
                return null;
            }

            try (//
                    var rowContainer = createRowIDContainer(); //
                    var writeCursor = rowContainer.createCursor(); //
            ) {
                progress.setMessage("Uniquify RowIDs");
                consumeRowIDs(table, rowID -> writeCursor.forward().setRowKey(uniquifyRowID(rowID)), progress);
                return rowContainer.finish();
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }

        private String uniquifyRowID(String rowID) {
            while (m_ids.contains(rowID)) {
                rowID += m_rowIDSuffix;
            }
            m_ids.add(rowID);
            return rowID;
        }

        private RowContainer createRowIDContainer() {
            var settings = DataContainerSettings.getDefault()//
                .withInitializedDomain(false)//
                .withRowKeysEnabled(false);
            return BACKEND.create(m_exec, new DataTableSpec(), settings, Node.invokeGetDataRepository(m_exec),
                Node.invokeGetFileStoreHandler(m_exec));
        }

    }

    private final class TablePrepper {

        private final Map<String, ValueFactory<?, ?>> m_valueFactoriesPerColumn;

        private final DataTableSpec m_concatenatedSpec;

        private final RowIDUniquifier m_rowIDUniquifier;

        private final List<ReferenceTable> m_referenceTables = new ArrayList<>();

        TablePrepper(final ColumnarValueSchema concatenatedSchema) {
            m_concatenatedSpec = concatenatedSchema.getSourceSpec();
            m_valueFactoriesPerColumn = new HashMap<>();
            for (int c = 0; c < m_concatenatedSpec.getNumColumns(); c++) {
                // +1 because DataTableSpec doesn't have the RowID as dedicated column
                m_valueFactoriesPerColumn.put(m_concatenatedSpec.getColumnSpec(c).getName(),
                    concatenatedSchema.getValueFactory(c + 1));
            }
            m_rowIDUniquifier = !m_checkForDuplicateIDs && m_rowIDSuffix != null ? new RowIDUniquifier() : null;
        }

        ReferenceTable[] getReferenceTables() {
            return m_referenceTables.toArray(ReferenceTable[]::new);
        }

        ColumnarVirtualTable prepare(final ReferenceTable table, final ExecutionMonitor progress)
            throws VirtualTableIncompatibleException {
            m_referenceTables.add(table);
            // we start a new fragment here
            var virtualTable = new ColumnarVirtualTable(table.getId(), table.getSchema(), true);
            var schema = table.getSchema();
            var spec = schema.getSourceSpec();
            virtualTable = castColumns(virtualTable);
            virtualTable = adjustRowIDs(table, progress, virtualTable);
            virtualTable = insertMissingColumns(spec, virtualTable);
            progress.setProgress(1);
            return virtualTable;
        }

        private ColumnarVirtualTable adjustRowIDs(final ReferenceTable table, final ExecutionMonitor progress,
            ColumnarVirtualTable virtualTable) throws VirtualTableIncompatibleException {
            if (m_rowIDUniquifier != null) {
                var rowIDTable = m_rowIDUniquifier.uniquifyRowIDs(table.getBufferedTable(), progress);
                if (rowIDTable != null) {
                    var rowIDRefTable = ReferenceTables.createReferenceTable(rowIDTable);
                    m_referenceTables.add(rowIDRefTable);
                    var rowIDSource = new ColumnarVirtualTable(rowIDRefTable.getId(), rowIDRefTable.getSchema(), true);
                    var virtualTableWithoutRowID =
                        virtualTable.selectColumns(IntStream.range(1, virtualTable.getSchema().numColumns()).toArray());
                    virtualTable = rowIDSource.append(List.of(virtualTableWithoutRowID));
                }
            }
            return virtualTable;
        }

        private ColumnarVirtualTable castColumns(ColumnarVirtualTable virtualTable) {
            var schema = virtualTable.getSchema();
            var spec = schema.getSourceSpec();
            var casts = new ArrayList<ColumnCast>();
            for (int c = 0; c < spec.getNumColumns(); c++) {
                var colSpec = spec.getColumnSpec(c);
                var concatenateValueFactory = m_valueFactoriesPerColumn.get(colSpec.getName());
                ValueFactory<ReadAccess, WriteAccess> valueFactory = schema.getValueFactory(c + 1);
                if (!ValueFactoryUtils.areEqual(concatenateValueFactory, valueFactory)) {
                    var outputColSpec = m_concatenatedSpec.getColumnSpec(colSpec.getName());
                    // in concatenate we always perform an upcast, so MapPath.DATA_VALUE is appropriate here
                    casts.add(new ColumnCast(c + 1, outputColSpec, new UntypedValueFactory(valueFactory),
                        new UntypedValueFactory(concatenateValueFactory), CastOperation.UPCAST));
                }
            }
            if (!casts.isEmpty()) {
                virtualTable = ColumnarSpecReplacer.cast(virtualTable, casts, m_fsHandler);
            }
            return virtualTable;
        }

        private ColumnarVirtualTable insertMissingColumns(final DataTableSpec spec, ColumnarVirtualTable virtualTable) {
            // append any missing columns and put them into the correct location
            var missingColumnSpecs = new ArrayList<DataColumnSpec>();
            var missingValueFactories = new ArrayList<ValueFactory<?, ?>>();
            var permutation = new int[m_concatenatedSpec.getNumColumns() + 1]; // +1 for the row key
            for (var c = 0; c < m_concatenatedSpec.getNumColumns(); c++) {
                var columnSpec = m_concatenatedSpec.getColumnSpec(c);
                var columnName = columnSpec.getName();
                if (spec.containsName(columnName)) {
                    // The column exists. Put it in the correct location (+1 for the row key)
                    permutation[c + 1] = spec.findColumnIndex(columnName) + 1;
                } else {
                    // The missing column will be appended after the existing columns in spec
                    // and after all other missing columns already found (+1 for the row key)
                    permutation[c + 1] = spec.getNumColumns() + missingColumnSpecs.size() + 1;
                    missingColumnSpecs.add(columnSpec);
                    missingValueFactories.add(m_valueFactoriesPerColumn.get(columnName));
                }
            }
            if (!missingColumnSpecs.isEmpty()) {
                var missingSchema = ColumnarValueSchemaUtils.create(
                    new DataTableSpec(missingColumnSpecs.toArray(DataColumnSpec[]::new)),
                    Stream.concat(Stream.of(VoidRowKeyFactory.INSTANCE), missingValueFactories.stream())
                        .toArray(ValueFactory[]::new));
                virtualTable = virtualTable.appendMissingValueColumns(missingSchema).selectColumns(permutation);
            } else if (IntStream.range(0, permutation.length).anyMatch(i -> i != permutation[i])) {
                virtualTable = virtualTable.selectColumns(permutation);
            }
            return virtualTable;
        }
    }

    private final class ColCreator {

        private final DataColumnSpecCreator m_specCreator;

        private ValueFactory<?, ?> m_valueFactory;


        ColCreator(final DataColumnSpec colSpec, final ValueFactory<?, ?> valueFactory) {
            m_specCreator = new DataColumnSpecCreator(colSpec);
            m_valueFactory = valueFactory;
        }

        void merge(final DataColumnSpec colSpec) {
            var oldType = m_specCreator.getType();
            m_specCreator.merge(colSpec, MERGE_OPTIONS);
            DataType newType = m_specCreator.getType();
            if (!oldType.equals(newType)) {
                m_valueFactory = ValueFactoryUtils.getValueFactory(newType, m_fsHandler);
            }
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
                var key = ex.getKey();
                throw new DuplicateKeyException(
                    "Duplicate RowID '%s' in table with index %s.".formatted(key, i), key);
            } catch (IOException ex) {
                throw new IllegalStateException("An I/O problem occurred while checking for duplicate RowIDs.", ex);
            }
        }
        try {
            checker.checkForDuplicates();
        } catch (DuplicateKeyException ex) {
            var key = ex.getKey();
            throw new DuplicateKeyException("Duplicate RowID '%s' detected.".formatted(key), key);
        } catch (IOException ex) {
            throw new IllegalStateException("An I/O problem occurred while checking for duplicate RowIDs.", ex);
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
