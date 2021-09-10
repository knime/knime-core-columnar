package org.knime.core.data.columnar.table;

import static java.util.stream.Collectors.toList;

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
 *   May 18, 2021 (dietzc): created
 */
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.filter.TableFilterUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.virtual.VirtualTableUtils;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueSchema;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.ExtensionTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.WorkflowDataRepository;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.exec.LazyVirtualTableExecutor;
import org.knime.core.table.virtual.exec.VirtualTableExecutor;
import org.knime.core.table.virtual.serialization.TableTransformSerializer;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 *
 * {@link ExtensionTable} implementation based on {@link VirtualTable VirtualTables}.
 *
 * @author Christian Dietz, KNIME GmbH, Stuttgart, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @since 4.5
 */
public final class VirtualTableExtensionTable extends ExtensionTable {

    private static final String CFG_SCHEMA = "schema";

    private static final String CFG_SIZE = "size";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(VirtualTableExtensionTable.class);

    private static final String CFG_REF_TABLES = "REF_TABLES";

    private static final String CFG_TRANSFORMSPECS = "SPECS";

    private final List<TableTransformSpec> m_tableTransformSpec;

    private final DataTableSpec m_dataTableSpec;

    private final ColumnarValueSchema m_schema;

    private final BufferedDataTable[] m_refTables;

    private final ReferenceTable[] m_referenceTables;

    private VirtualTable m_virtualTable;

    private List<RowAccessible> m_cachedOutputs;

    private final long m_size;

    private final int m_tableId;

    /**
     * Serialization constructor. Not to be used by clients.
     *
     * @param context of the load operation
     * @throws InvalidSettingsException if the stored settings are invalid
     */
    public VirtualTableExtensionTable(final LoadContext context) throws InvalidSettingsException {
        final NodeSettingsRO settings = context.getSettings();
        // only relevant for newly created tables that are temporary i.e. not output tables of a node (see AP-15779)
        // If this table is loaded it means that it must be either an output of some node
        m_tableId = -1;
        m_schema = ColumnarValueSchemaUtils.create(ValueSchema.Serializer.load(context.getTableSpec(),
            context.getDataRepository(), settings.getNodeSettings(CFG_SCHEMA)));
        m_dataTableSpec = context.getTableSpec();
        int[] refIds = settings.getIntArray(CFG_REF_TABLES);
        m_refTables = new BufferedDataTable[refIds.length];
        m_referenceTables = new ReferenceTable[refIds.length];
        m_size = settings.getLong(CFG_SIZE);
        for (var i = 0; i < refIds.length; i++) {
            final BufferedDataTable refTable = context.getTable(refIds[i]);
            m_refTables[i] = refTable;
            final var fsHandler = NotInWorkflowWriteFileStoreHandler.create();
            fsHandler.addToRepository(context.getDataRepository());
            m_referenceTables[i] = createReferenceTable(refTable, fsHandler);
        }
        try {
            m_tableTransformSpec = reconstructSpecsFromStringArray(settings.getStringArray(CFG_TRANSFORMSPECS));
        } catch (IOException ex) {
            throw new InvalidSettingsException("Error while deserializing transformation ", ex);
        }
    }

    private static List<TableTransformSpec> reconstructSpecsFromStringArray(final String[] serializedSpecs)
        throws JsonProcessingException {
        final List<TableTransformSpec> specs = new ArrayList<>(serializedSpecs.length);
        var objectMapper = new ObjectMapper();
        for (String serializedSpec : serializedSpecs) {
            TableTransformSpec spec =
                TableTransformSerializer.deserializeTransformSpec(objectMapper.readTree(serializedSpec));
            specs.add(spec);
        }
        return specs;
    }

    /**
     * Constructor.
     *
     * @param refs the reference tables
     * @param fsHandler the {@link IWriteFileStoreHandler}
     * @param specs the transformations to apply
     * @param transformedSpec the {@link DataTableSpec} AFTER the transformations are applied
     * @param size the size of the output table AFTER the transformations are applied
     * @param tableId the id with which this table is tracked
     */
    public VirtualTableExtensionTable(final BufferedDataTable[] refs, //
        final IWriteFileStoreHandler fsHandler, //
        final List<TableTransformSpec> specs, //
        final DataTableSpec transformedSpec, //
        final long size,//
        final int tableId) {
        m_tableId = tableId;
        m_tableTransformSpec = specs;
        m_refTables = refs;
        m_referenceTables = Arrays.stream(refs)//
            .map(t -> createReferenceTable(t, fsHandler))//
            .toArray(ReferenceTable[]::new);
        m_schema = ColumnarValueSchemaUtils.create(ValueSchema.create(transformedSpec, RowKeyType.CUSTOM, fsHandler));
        m_dataTableSpec = m_schema.getSourceSpec();
        m_size = size;
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        m_schema.save(settings.addNodeSettings(CFG_SCHEMA));
        settings.addStringArray(CFG_TRANSFORMSPECS, getSpecsAsStringArray());
        final var ids = new int[m_refTables.length];
        for (var i = 0; i < ids.length; i++) {
            ids[i] = m_refTables[i].getBufferedTableId();
        }
        settings.addIntArray(CFG_REF_TABLES, ids);
        settings.addLong(CFG_SIZE, m_size);
    }

    private String[] getSpecsAsStringArray() throws JsonProcessingException {
        final var mapper = new ObjectMapper();
        final var factory = new JsonNodeFactory(false);
        final var strings = new String[m_tableTransformSpec.size()];
        var i = 0;
        for (TableTransformSpec spec : m_tableTransformSpec) {
            strings[i] = mapper.writeValueAsString(TableTransformSerializer.serializeTransformSpec(spec, factory));
            i++;
        }
        return strings;
    }

    private synchronized VirtualTable getVirtualTable() {
        // lazily setup graph once
        if (m_virtualTable == null) {
            m_virtualTable = initializeVirtualTable();
        }

        return m_virtualTable;
    }

    private VirtualTable initializeVirtualTable() {
        var tableTransform = buildGraph();
        return new VirtualTable(tableTransform, m_schema);
    }

    private TableTransform buildGraph() {
        final List<TableTransform> parents = Arrays.stream(m_referenceTables)//
            .map(ReferenceTable::getParent)//
            .collect(toList());
        Iterator<TableTransformSpec> transformSpecs = m_tableTransformSpec.iterator();
        var tableTransform = new TableTransform(parents, transformSpecs.next());
        while (transformSpecs.hasNext()) {
            tableTransform = new TableTransform(List.of(tableTransform), transformSpecs.next());
        }
        return tableTransform;
    }

    @Override
    public int getTableId() {
        return m_tableId;
    }

    @Override
    public DataTableSpec getDataTableSpec() {
        return m_dataTableSpec;
    }

    @Deprecated
    @Override
    public int getRowCount() { //NOSONAR must be implemented because it's in the API
        return KnowsRowCountTable.checkRowCount(size());
    }

    @Override
    public long size() {
        return m_size;
    }

    @Override
    public void clear() {
        m_virtualTable = null;
        clearOutputCache();
    }

    private void clearOutputCache() {
        if (m_cachedOutputs != null) {
            for (RowAccessible output : m_cachedOutputs) {
                try {
                    output.close();
                } catch (IOException ex) {
                    LOGGER.debug("Failed to close the cached output table.", ex);
                }
            }
            m_cachedOutputs = null;
        }
    }

    @Override
    public void ensureOpen() {
        // NB: We directly read from workspace and don't copy data to temp for reading. Therefore: Noop.
    }

    @Override
    public BufferedDataTable[] getReferenceTables() {
        return m_refTables;
    }

    @Override
    public void putIntoTableRepository(final WorkflowDataRepository dataRepository) {
        // only relevant in case of tables that hold data of their own
    }

    @Override
    public boolean removeFromTableRepository(final WorkflowDataRepository dataRepository) {
        // only relevant in case of newly created tables
        if (!dataRepository.removeTable(getTableId()).isPresent()) {
            LOGGER.debugWithFormat("Failed to remove container table with id %d from global table repository.",
                getTableId());
            return false;
        }
        return true;
    }

    @SuppressWarnings("resource") // the cursor is managed by the returned iterator
    @Override
    public CloseableRowIterator iterator() {
        return new ColumnarRowIterator(cursor());
    }

    @SuppressWarnings("resource") // the cursor is managed by the returned RowCursor
    @Override
    public RowCursor cursor() {
        return VirtualTableUtils.createColumnarRowCursor(m_schema, getOutput().createCursor());
    }

    private synchronized RowAccessible getOutput() {
        if (m_cachedOutputs == null) {
            m_cachedOutputs = runComputationGraph();
        }
        return m_cachedOutputs.get(0);
    }

    private boolean hasCachedOutput() {//NOSONAR
        return m_cachedOutputs != null;
    }

    private List<RowAccessible> runComputationGraph() {
        final VirtualTableExecutor exec = new LazyVirtualTableExecutor(getVirtualTable().getProducingTransform());
        final Map<UUID, RowAccessible> sources = collectSources();
        return exec.execute(sources);
    }

    private Map<UUID, RowAccessible> collectSources() {
        final Map<UUID, RowAccessible> sources = new HashMap<>();
        for (var i = 0; i < m_referenceTables.length; i++) {
            sources.putAll(m_referenceTables[i].getSources());
        }
        return sources;
    }

    @Override
    public RowCursor cursor(final TableFilter filter) {
        if (TableFilterUtils.hasFilter(filter)) {
            return createdFilteredRowCursor(filter);
        } else {
            return cursor();
        }
    }

    @SuppressWarnings("resource") // the cursor will be closed by the returned RowCursor
    private RowCursor createdFilteredRowCursor(final TableFilter filter) {
        final var srcId = UUID.randomUUID();
        var table = new VirtualTable(srcId, m_schema);

        if (TableFilterUtils.definesRowRange(filter)) {
            table = table.slice(TableFilterUtils.extractFromIndex(filter),
                TableFilterUtils.extractToIndex(filter, m_size) + 1);
        }

        if (TableFilterUtils.definesColumnFilter(filter)) {
            table = table.filterColumns(TableFilterUtils.extractPhysicalColumnIndices(filter, m_schema.numColumns()));
        }
        final VirtualTableExecutor exec = new LazyVirtualTableExecutor(table.getProducingTransform());
        final List<RowAccessible> accessibles = exec.execute(Map.of(srcId, getOutput()));
        final Cursor<ReadAccessRow> physicalCursor = accessibles.get(0).createCursor();
        return TableFilterUtils.createColumnSelection(filter, m_schema.numColumns())//
            .map(s -> VirtualTableUtils.createTableFilterRowCursor(m_schema, physicalCursor, s))//
            .orElseGet(() -> VirtualTableUtils.createColumnarRowCursor(m_schema, physicalCursor));
    }

    @SuppressWarnings("resource")
    private static ReferenceTable createReferenceTable(final BufferedDataTable table,
        final IWriteFileStoreHandler fsHandler) {
        final ExtensionTable unwrapped = unwrap(table);
        if (unwrapped instanceof VirtualTableExtensionTable) {
            final VirtualTableExtensionTable virtualExtensionTable = (VirtualTableExtensionTable)unwrapped;
            return new VirtualReferenceTable(virtualExtensionTable);
        } else if (unwrapped instanceof AbstractColumnarContainerTable) {
            final AbstractColumnarContainerTable columnarTable = (AbstractColumnarContainerTable)unwrapped;
            return new ColumnarContainerReferenceTable(columnarTable);
        } else {
            // we end up here if the reference tables are not extension tables (e.g. RearrangeColumnsTable)
            return new BufferedReferenceTable(table, fsHandler);
        }
    }

    private interface ReferenceTable {

        TableTransform getParent();

        Map<UUID, RowAccessible> getSources();

    }

    private static final class VirtualReferenceTable implements ReferenceTable {

        private final VirtualTableExtensionTable m_table;

        private UUID m_id;

        VirtualReferenceTable(final VirtualTableExtensionTable table) {
            m_table = table;
        }

        @Override
        public TableTransform getParent() {
            if (m_table.hasCachedOutput()) {
                assert m_id == null;
                m_id = UUID.randomUUID();
                // TODO discuss if we should always take this route
                // it would have the benefit that upstream nodes wouldn't have to run the comp graph again if a
                // downstream node already ran it but it might be slower because we would run the partial
                // comp graph for every ancestor that is a VirtualTableExtensionTable
                return new TableTransform(new SourceTransformSpec(m_id));
            } else {
                return m_table.getVirtualTable().getProducingTransform();
            }
        }

        @SuppressWarnings("resource")
        @Override
        public Map<UUID, RowAccessible> getSources() {
            if (m_id != null) {
                assert m_table.hasCachedOutput();
                // we need to prevent the returned RandomAccessible from being closed because otherwise resetting
                // a downstream node would close the underlying RowAccessible of an upstream node
                return Map.of(m_id, VirtualTableUtils.uncloseable(m_table.getOutput()));
            }
            return m_table.collectSources();
        }

    }

    private static final class ColumnarContainerReferenceTable implements ReferenceTable {

        private final AbstractColumnarContainerTable m_table;

        private final UUID m_id = UUID.randomUUID();

        ColumnarContainerReferenceTable(final AbstractColumnarContainerTable table) {
            m_table = table;
        }

        @Override
        public TableTransform getParent() {
            return new TableTransform(new SourceTransformSpec(m_id));
        }

        @SuppressWarnings("resource") // we close the RowAccessible by closing m_cachedOutput
        @Override
        public Map<UUID, RowAccessible> getSources() {
            return Collections.singletonMap(m_id, m_table.asRowAccessible());
        }
    }

    private static final class BufferedReferenceTable implements ReferenceTable {

        private final BufferedDataTable m_table;

        private final UUID m_id = UUID.randomUUID();

        private final ColumnarValueSchema m_schema;

        BufferedReferenceTable(final BufferedDataTable table, final IWriteFileStoreHandler fsHandler) {
            m_table = table;
            final var schema = ValueSchema.create(table.getDataTableSpec(), RowKeyType.CUSTOM, fsHandler);
            m_schema = ColumnarValueSchemaUtils.create(schema);
        }

        @Override
        public TableTransform getParent() {
            return new TableTransform(new SourceTransformSpec(m_id));
        }

        // the returned RowAccessible will be closed through the computation graph when we close m_cachedOutputs
        @SuppressWarnings("resource")
        @Override
        public Map<UUID, RowAccessible> getSources() {
            return Collections.singletonMap(m_id, VirtualTableUtils.createRowAccessible(m_schema, m_table));
        }

    }
}
