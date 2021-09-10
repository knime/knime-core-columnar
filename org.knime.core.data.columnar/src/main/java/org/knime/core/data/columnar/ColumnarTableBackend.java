
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.TableBackend;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ColumnarRowContainerSettings;
import org.knime.core.data.columnar.table.ColumnarRowContainerUtils;
import org.knime.core.data.columnar.table.VirtualTableExtensionTable;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.ColumnRearrangerUtils;
import org.knime.core.data.container.ConcatenateTable;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.container.ILocalDataRepository;
import org.knime.core.data.container.RearrangeColumnsTable;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueSchema;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

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

    private static final String DESCRIPTION;

    static {
        String d;
        try (InputStream in = ColumnarTableBackend.class.getResourceAsStream("description.html")) {
            d = IOUtils.readLines(in, StandardCharsets.UTF_8).stream().collect(Collectors.joining("\n"));
        } catch (NullPointerException | IOException ioe) { // NOSONAR
            LOGGER.error("Unable to parse description file", ioe);
            d = "";
        }
        DESCRIPTION = d;
    }

    @Override
    public DataContainerDelegate create(final DataTableSpec spec, final DataContainerSettings settings,
        final IDataRepository repository, final ILocalDataRepository localRepository,
        final IWriteFileStoreHandler fileStoreHandler) {
        final ValueSchema schema =
            ValueSchema.create(spec, settings.isEnableRowKeys() ? RowKeyType.CUSTOM : RowKeyType.NOKEY,
                initFileStoreHandler(fileStoreHandler, repository));
        final ColumnarValueSchema columnarSchema = ColumnarValueSchemaUtils.create(schema);
        try {
            final ColumnarRowContainerSettings cursorSettings = new ColumnarRowContainerSettings(
                settings.getInitializeDomain(), settings.getMaxDomainValues(), settings.isEnableRowKeys(),
                settings.isForceSequentialRowHandling());
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
                ValueSchema.create(spec, RowKeyType.CUSTOM, initFileStoreHandler(handler, repository));
            final ColumnarRowContainerSettings containerSettings = new ColumnarRowContainerSettings(
                settings.getInitializeDomain(), settings.getMaxDomainValues(), settings.isEnableRowKeys(),
                settings.isForceSequentialRowHandling());
            return ColumnarRowContainerUtils.create(context, -1, ColumnarValueSchemaUtils.create(schema),
                containerSettings);
        } catch (Exception e) {
            throw new IllegalStateException("Exception while creating ColumnarRowWriteCursor.", e);
        }
    }

    @Override
    public String getShortName() {
        return "Columnar Storage (Labs)";
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
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
    public KnowsRowCountTable concatenate(final ExecutionMonitor exec, final IWriteFileStoreHandler filestoreHandler,
        final IntSupplier tableIdSupplier, final String rowKeyDuplicateSuffix, final boolean duplicatesPreCheck, final BufferedDataTable... tables)
        throws CanceledExecutionException {
        if (duplicatesPreCheck && rowKeyDuplicateSuffix == null) {
            TableTransformUtils.checkForDuplicateKeys(exec, tables);
            final DataTableSpec concatenateSpec = TableTransformUtils.concatenateSpec(tables);
            final long concatenatedSize = TableTransformUtils.concatenatedSize(tables);
            return new VirtualTableExtensionTable(tables, filestoreHandler, List.of(new ConcatenateTransformSpec()),
                concatenateSpec, concatenatedSize, tableIdSupplier.getAsInt());
        } else {
            return ConcatenateTable.create(exec, Optional.ofNullable(rowKeyDuplicateSuffix), duplicatesPreCheck,
                tables);
        }
    }

    @Override
    public KnowsRowCountTable append(final ExecutionMonitor exec, final IWriteFileStoreHandler filestoreHandler,
        final IntSupplier tableIdSupplier, final BufferedDataTable left, final BufferedDataTable right) throws CanceledExecutionException {
        final BufferedDataTable[] tables = {left, right};
        TableTransformUtils.checkRowKeysMatch(exec, tables);
        final long appendSize = TableTransformUtils.appendSize(tables);
        final DataTableSpec appendedSpec = TableTransformUtils.appendSpec(tables);
        return new VirtualTableExtensionTable(tables, filestoreHandler,
            TableTransformUtils.createAppendTransformations(tables), appendedSpec, appendSize, tableIdSupplier.getAsInt());
    }

    @Override
    public KnowsRowCountTable rearrange(final ExecutionMonitor progressMonitor,
        final IWriteFileStoreHandler filestoreHandler, final IntSupplier tableIdSupplier,
        final ColumnRearranger columnRearranger, final BufferedDataTable table, final ExecutionContext context) throws CanceledExecutionException {
        ColumnRearrangerUtils.checkSpecCompatibility(columnRearranger, table.getDataTableSpec());
        if (ColumnRearrangerUtils.addsNoNewColumns(columnRearranger)) {
            List<TableTransformSpec> transformations = TableTransformUtils.createRearrangeTransformations(
                ColumnRearrangerUtils.extractOriginalIndicesOfIncludedColumns(columnRearranger),
                table.getDataTableSpec().getNumColumns());
            return new VirtualTableExtensionTable(new BufferedDataTable[]{table}, filestoreHandler, transformations,
                columnRearranger.createSpec(), table.size(), tableIdSupplier.getAsInt());
        } else {
            // TODO replace fallback with fast tables based implementation once we have map functionality
            return RearrangeColumnsTable.create(columnRearranger, table, progressMonitor, context);
        }
    }

}
