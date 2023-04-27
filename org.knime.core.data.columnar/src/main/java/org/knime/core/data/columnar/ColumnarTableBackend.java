
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
 */
package org.knime.core.data.columnar;

import static org.knime.core.data.columnar.table.virtual.reference.ReferenceTables.createReferenceTables;

import java.util.UUID;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.TableBackend;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarRowContainerUtils;
import org.knime.core.data.columnar.table.ColumnarRowWriteTableSettings;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.columnar.table.VirtualTableIncompatibleException;
import org.knime.core.data.columnar.table.VirtualTableSchemaUtils;
import org.knime.core.data.columnar.table.virtual.ColumnarConcatenater;
import org.knime.core.data.columnar.table.virtual.ColumnarRearranger;
import org.knime.core.data.columnar.table.virtual.ColumnarSpecReplacer;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.container.BufferedTableBackend;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.container.ILocalDataRepository;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.row.Selection;
import org.knime.core.table.virtual.VirtualTable;

/**
 * Columnar {@link TableBackend} implementation.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Bernd Wiswedel, KNIME GmbH, Konstanz, Germany
 *
 * @since 4.3
 */
public final class ColumnarTableBackend implements TableBackend {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarTableBackend.class);

    private static final BufferedTableBackend OLD_BACKEND = new BufferedTableBackend();

    @Override
    public DataContainerDelegate create(final DataTableSpec spec, final DataContainerSettings settings,
        final IDataRepository repository, final ILocalDataRepository localRepository,
        final IWriteFileStoreHandler fileStoreHandler) {
        final ValueSchema schema =
            ValueSchemaUtils.create(spec, settings.isEnableRowKeys() ? RowKeyType.CUSTOM : RowKeyType.NOKEY,
                initFileStoreHandler(fileStoreHandler, repository));
        final ColumnarValueSchema columnarSchema = ColumnarValueSchemaUtils.create(schema);
        try {
            final ColumnarRowWriteTableSettings cursorSettings =
                new ColumnarRowWriteTableSettings(settings.getInitializeDomain(), settings.getMaxDomainValues(),
                    settings.isEnableRowKeys(), settings.isForceSequentialRowHandling(), settings.getRowBatchSize(), maxPendingBatches(settings));
            return ColumnarRowContainerUtils.create(repository.generateNewID(), columnarSchema, cursorSettings);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create DataContainerDelegate for ColumnarTableBackend.", e);
        }
    }

    @Override
    public RowContainer create(final ExecutionContext context, final DataTableSpec spec,
        final DataContainerSettings settings, final IDataRepository repository, final IWriteFileStoreHandler handler) {
        try {
            final ValueSchema schema =
                ValueSchemaUtils.create(spec, RowKeyType.CUSTOM, initFileStoreHandler(handler, repository));
            final ColumnarRowWriteTableSettings containerSettings =
                new ColumnarRowWriteTableSettings(settings.getInitializeDomain(), settings.getMaxDomainValues(),
                    settings.isEnableRowKeys(), settings.isForceSequentialRowHandling(), settings.getRowBatchSize(), maxPendingBatches(settings));
            return ColumnarRowContainerUtils.create(context, -1, ColumnarValueSchemaUtils.create(schema),
                containerSettings);
        } catch (Exception e) {
            throw new IllegalStateException("Exception while creating ColumnarRowWriteCursor.", e);
        }
    }

    private static int maxPendingBatches( final DataContainerSettings settings ) {
        // TODO: Empirically find good setting for maxPendingBatches
        return settings.getMaxContainerThreads();
    }

    @Override
    public String getShortName() {
        return "Columnar Backend";
    }

    @Override
    public String getDescription() {
        return "The Columnar Table Backend extension uses a different\n"
            + "underlying data layer based on a columnar representation,\n"
            + "which gives noticable speed-ups over the default format.\n\n"
            + "Please also review the settings in the KNIME preferences\n"
            + "(File -> Preferences -> KNIME -> Table Backend -> Columnar Backend)\n";
    }

    // TODO required? how can it ever be null?
    private static IWriteFileStoreHandler initFileStoreHandler(final IWriteFileStoreHandler fileStoreHandler,
        final IDataRepository repository) {
        IWriteFileStoreHandler nonNull = fileStoreHandler;
        if (nonNull == null) {
            nonNull = NotInWorkflowWriteFileStoreHandler.create();
            nonNull.addToRepository(repository);
        }
        return nonNull;
    }

    @Override
    public KnowsRowCountTable concatenate(final ExecutionContext exec, final ExecutionMonitor progressMonitor,
        final IntSupplier tableIdSupplier, final String rowKeyDuplicateSuffix, final boolean duplicatesPreCheck,
        final BufferedDataTable... tables) throws CanceledExecutionException {
        try {
            return new ColumnarConcatenater(tableIdSupplier, exec, duplicatesPreCheck, rowKeyDuplicateSuffix)
                .concatenate(tables, progressMonitor);
        } catch (VirtualTableIncompatibleException ex) {//NOSONAR don't spam the log
            LOGGER.debug("Can't concatenate tables with Columnar Table Backend, falling back on the old backend.");
            return OLD_BACKEND.concatenate(null, exec, tableIdSupplier, rowKeyDuplicateSuffix, duplicatesPreCheck,
                tables);
        }
    }

    @Override
    public KnowsRowCountTable append(final ExecutionContext exec, final IntSupplier tableIdSupplier,
        final AppendConfig config, final BufferedDataTable left, final BufferedDataTable right)
        throws CanceledExecutionException {
        try {
            switch (config.getRowIDMode()) {
                case FROM_TABLE:
                        var table = appendWithRowIDFromTable(tableIdSupplier, config.getRowIDTableIndex(),
                            new BufferedDataTable[]{left, right});
                        exec.setProgress(1);
                        return table;
                case MATCHING:
                    return append(exec, tableIdSupplier, left, right);
                default:
                    throw new IllegalStateException("Unknown RowIDMode encountered: " + config.getRowIDMode());
            }
        } catch (VirtualTableIncompatibleException ex) {
            LOGGER.debug("Can't append with the Columnar Table Backend, falling back on the old backend.");
            return OLD_BACKEND.append(exec, tableIdSupplier, config, left, right);
        }
    }

    private static KnowsRowCountTable appendWithRowIDFromTable(
        final IntSupplier tableIdSupplier, final int tableIndex, final BufferedDataTable[] tables)
        throws VirtualTableIncompatibleException {
        var appendedSchema = VirtualTableSchemaUtils.appendSchemas(tables);
        var refTables = createReferenceTables(tables);
        return new VirtualTableExtensionTable(refTables, TableTransformUtils.appendTables(refTables, tableIndex),
            appendedSchema, tables[0].size(), tableIdSupplier.getAsInt());
    }

    @Override
    public KnowsRowCountTable append(final ExecutionMonitor exec, final IntSupplier tableIdSupplier,
        final BufferedDataTable left, final BufferedDataTable right) throws CanceledExecutionException {
        final BufferedDataTable[] tables = {left, right};
        try {
            var appendedSchema = VirtualTableSchemaUtils.appendSchemas(tables);
            TableTransformUtils.checkRowKeysMatch(exec, tables);
            final long appendSize = TableTransformUtils.appendSize(tables);
            var refTables = createReferenceTables(tables);
            return new VirtualTableExtensionTable(refTables, TableTransformUtils.appendTables(refTables, 0),
                appendedSchema, appendSize, tableIdSupplier.getAsInt());
        } catch (VirtualTableIncompatibleException ex) {// NOSONAR don't spam the log
            LOGGER.debug("Can't append with the Columnar Table Backend, falling back on the old backend.");
            return OLD_BACKEND.append(exec, tableIdSupplier, left, right);
        }
    }


    @Override
    public KnowsRowCountTable rearrange(final ExecutionMonitor progressMonitor, final IntSupplier tableIdSupplier,
        final ColumnRearranger columnRearranger, final BufferedDataTable table, final ExecutionContext context)
        throws CanceledExecutionException {
        try {
            return new ColumnarRearranger(context, tableIdSupplier).transform(progressMonitor, columnRearranger, table);
        } catch (VirtualTableIncompatibleException ex) {//NOSONAR don't spam the log
            LOGGER.debug("Can't run ColumnRearranger on the Columnar Table Backend. Falling back on the old backend.");
            return OLD_BACKEND.rearrange(progressMonitor, tableIdSupplier, columnRearranger, table, context);
        }
    }

    @Override
    public KnowsRowCountTable slice(final ExecutionContext exec, final BufferedDataTable table, final Selection slice,
        final IntSupplier tableIdSupplier) {
        var spec = table.getDataTableSpec();
        try {
            var refTable = ReferenceTables.createReferenceTable(UUID.randomUUID(), table);
            var virtualTable = new VirtualTable(refTable.getId(), refTable.getSchema());
            var cols = slice.columns();
            var rearranger = new ColumnRearranger(spec);
            if (!cols.allSelected()){
                var colIndices = cols.getSelected();
                virtualTable = virtualTable.filterColumns(//
                    IntStream.concat(//
                        IntStream.of(0), // the row key is always part of the table
                        IntStream.of(colIndices)//
                            .map(i -> i + 1))// the slice does not account for the row key column
                        .toArray()//
                );
                rearranger.keepOnly(colIndices);
            }

            var rows = slice.rows();
            var size = table.size();
            if (!rows.allSelected(0, size)) {
                long fromRow = Math.max(0, rows.fromIndex());
                long toRow = Math.min(size, rows.toIndex());
                virtualTable = virtualTable.slice(fromRow, toRow);
                size = toRow - fromRow;
            }

            var rearrangedSchema = VirtualTableSchemaUtils.rearrangeSchema(table, rearranger);

            var slicedTable = new VirtualTableExtensionTable(new ReferenceTable[]{refTable}, virtualTable,
                rearrangedSchema, size, tableIdSupplier.getAsInt());
            exec.setProgress(1);
            return slicedTable;
        } catch (VirtualTableIncompatibleException ex) {//NOSONAR
            return OLD_BACKEND.slice(exec, table, slice, tableIdSupplier);
        }
    }

    @Override
    public KnowsRowCountTable replaceSpec(final ExecutionContext exec, final BufferedDataTable table,
        final DataTableSpec newSpec, final IntSupplier tableIDSupplier) {
        try {
            return new ColumnarSpecReplacer(exec, tableIDSupplier).replaceSpec(table, newSpec);
        } catch (VirtualTableIncompatibleException ex) {
            return OLD_BACKEND.replaceSpec(exec, table, newSpec, tableIDSupplier);
        }
    }

}
