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
package org.knime.core.data.columnar.table;

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.filter.TableFilterUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable;
import org.knime.core.data.columnar.table.virtual.VirtualTableUtils;
import org.knime.core.data.columnar.table.virtual.persist.TableTransformNodeSettingsPersistor;
import org.knime.core.data.columnar.table.virtual.persist.TableTransformSerializer;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTables;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.ExtensionTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.WorkflowDataRepository;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.exec.GraphVirtualTableExecutor;
import org.knime.core.table.virtual.exec.VirtualTableExecutor;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * {@link ExtensionTable} implementation based on {@link VirtualTable VirtualTables}.
 *
 * @author Christian Dietz, KNIME GmbH, Stuttgart, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @since 4.5
 */
public final class VirtualTableExtensionTable extends ExtensionTable {

    private static final String CFG_VIRTUAL_TABLE_FRAGMENT_PRE_5_1 = "VIRTUAL_TABLE_FRAGMENT";

    private static final String CFG_VIRTUAL_TABLE_FRAGMENT = "virtual_table_fragment_5_1";

    private static final String CFG_REF_TABLE_IDS = "REF_TABLE_IDS";

    private static final String CFG_SCHEMA = "schema";

    private static final String CFG_SIZE = "size";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(VirtualTableExtensionTable.class);

    private static final String CFG_REF_TABLES = "REF_TABLES";

    private static final String CFG_TRANSFORMSPECS = "SPECS";

    private final DataTableSpec m_dataTableSpec;

    private final ColumnarValueSchema m_schema;

    private final BufferedDataTable[] m_refTables;

    private final ReferenceTable[] m_referenceTables;

    private List<RowAccessible> m_cachedOutputs;

    /**
     * Fragment of the workflow's VirtualTable that corresponds to this VirtualTableExtensionTable. Contains all
     * transformations, that are saved as part of this VirtualTableExtensionTable.
     */
    private final TableTransform m_tableTransformFragment;

    /**
     * The workflow's full VirtualTable up to this table. This is not saved but constructed using m_virtualTableFragment
     * and the m_referenceTables. Allows us to optimize the full VirtualTable for requests to this table's data.
     *
     */
    private final TableTransform m_resolvedTableTransform;

    private final long m_size;

    private final int m_tableId;

    private final CursorTracker<RowCursor> m_openCursors = CursorTracker.createRowCursorTracker();

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
        m_dataTableSpec = context.getTableSpec();
        int[] refIds = settings.getIntArray(CFG_REF_TABLES);
        m_refTables = new BufferedDataTable[refIds.length];
        m_size = settings.getLong(CFG_SIZE);
        for (var i = 0; i < refIds.length; i++) {
            final BufferedDataTable refTable = context.getTable(refIds[i]);
            m_refTables[i] = refTable;
        }

        try {
            m_referenceTables = createReferenceTables(settings, m_refTables);
        } catch (VirtualTableIncompatibleException ex) {
            throw new IllegalStateException("The loaded reference tables are incompatible with virtual tables. This is "
                + "most likely an implementation error because they must have been compatible when storing the data.",
                ex);
        }
        try {
            m_tableTransformFragment = loadTableTransformFragment(settings, m_referenceTables, context);
        } catch (IOException ex) {
            throw new InvalidSettingsException("Error while deserializing transformation ", ex);
        }
        m_resolvedTableTransform = resolveFullTableTransform(m_tableTransformFragment, m_referenceTables);

        @SuppressWarnings("resource") // will be closed in the close method
        var columnarSchema = getOutput().getSchema();
        m_schema = ColumnarValueSchemaUtils.load(columnarSchema, context);
    }

    private static ReferenceTable[] createReferenceTables(final NodeSettingsRO settings, final BufferedDataTable[] refTables)
        throws InvalidSettingsException, VirtualTableIncompatibleException {
        final var numRefTables = refTables.length;
        var referenceTables = new ReferenceTable[numRefTables];
        if (settings.containsKey(CFG_REF_TABLE_IDS)) {
            var ids = settings.getStringArray(CFG_REF_TABLE_IDS);
            final var numIds = ids.length;
            CheckUtils.checkSetting(numIds == numRefTables,
                "The number of reference tables (%s) and reference table IDs (%s) vary.", numRefTables, numIds);
            for (int i = 0; i < numIds; i++) { //NOSONAR
                var id = UUID.fromString(ids[i]);
                referenceTables[i] = ReferenceTables.createReferenceTable(id, refTables[i]);
            }
        } else {
            // no ids present (probably stored with 4.5) -> generate new ids
            for (int i = 0; i < numRefTables; i++) { //NOSONAR
                referenceTables[i] = ReferenceTables.createReferenceTable(UUID.randomUUID(), refTables[i]);
            }
        }
        return referenceTables;
    }

    private static TableTransform loadTableTransformFragment(final NodeSettingsRO settings,
        final ReferenceTable[] referenceTables, final LoadContext ctx)
        throws JsonProcessingException, InvalidSettingsException {
        var objectMapper = new ObjectMapper();
        if (settings.containsKey(CFG_VIRTUAL_TABLE_FRAGMENT)) {
            return TableTransformNodeSettingsPersistor.load(settings.getNodeSettings(CFG_VIRTUAL_TABLE_FRAGMENT),
                ctx::getDataRepository);
        } else if (settings.containsKey(CFG_VIRTUAL_TABLE_FRAGMENT_PRE_5_1)) {
            // before 5.1 we stored the transform as JSON
            return TableTransformSerializer
                .load(objectMapper.readTree(settings.getString(CFG_VIRTUAL_TABLE_FRAGMENT_PRE_5_1)));
        } else {
            // in 4.5 we stored a list of TableTransformSpecs as opposed to a complete TableTransform or VirtualTable
            var transformSpecs = reconstructSpecsFromStringArray(settings.getStringArray(CFG_TRANSFORMSPECS));
            var sourceSpecs = Stream.of(referenceTables)//
                    // all KNIME tables know their size and therefore are LookaheadRowAccessibles
                    .map(t -> new SourceTransformSpec(t.getId(), new SourceTableProperties(t.getSchema(), true)))//
                    .map(TableTransform::new)//
                    .collect(toList());
            var transformSpecIter = transformSpecs.iterator();
            CheckUtils.checkSetting(transformSpecIter.hasNext(), "No transform spec stored");
            var tableTransform = new TableTransform(sourceSpecs, transformSpecIter.next());
            while (transformSpecIter.hasNext()) {
                // in 4.5 we always had a simple sequence of transformations without any branching
                tableTransform = new TableTransform(List.of(tableTransform), transformSpecIter.next());
            }
            return tableTransform;
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
     * @param virtualTableFragment the {@link VirtualTable} representing this table with the {@link ReferenceTable refs}
     *            as sources
     * @param transformedSchema the {@link ColumnarValueSchema} AFTER the transformations are applied
     * @param size the size of the output table AFTER the transformations are applied
     * @param tableId the id with which this table is tracked
     */
    public VirtualTableExtensionTable(final ReferenceTable[] refs, //
        final VirtualTable virtualTableFragment, //
        final ColumnarValueSchema transformedSchema, //
        final long size, //
        final int tableId) {
        this(refs, virtualTableFragment.getProducingTransform(), transformedSchema, size, tableId);
    }

    /**
     * Constructor.
     *
     * @param refs the reference tables
     * @param virtualTableFragment the {@link VirtualTable} representing this table with the {@link ReferenceTable refs}
     *            as sources
     * @param size the size of the output table AFTER the transformations are applied
     * @param tableId the id with which this table is tracked
     */
    public VirtualTableExtensionTable(final ReferenceTable[] refs, //
        final ColumnarVirtualTable virtualTableFragment, //
        final long size, //
        final int tableId) {
        this(refs, virtualTableFragment.getProducingTransform(), virtualTableFragment.getSchema(), size, tableId);
    }

    private VirtualTableExtensionTable(final ReferenceTable[] refs, //
        final TableTransform fragmentTableTransform, //
        final ColumnarValueSchema transformedSchema, //
        final long size, //
        final int tableId) {
        m_tableId = tableId;
        m_tableTransformFragment = fragmentTableTransform;
        m_resolvedTableTransform = resolveFullTableTransform(m_tableTransformFragment, refs);
        m_refTables = Stream.of(refs)//
            .map(ReferenceTable::getBufferedTable)//
            .toArray(BufferedDataTable[]::new);
        m_referenceTables = refs;
        // TODO we could derive the schema from the reference tables but this entails running the computation graph
        // NOTE: The computation graph does not perform I/O
        // Any input table dependent checking would still have to happen outside of this constructor
        // or the VirtualTableExecutor must somehow be configured to do the checking
        // e.g. via a visitor for the different transformation types
        m_schema = transformedSchema;
        m_dataTableSpec = m_schema.getSourceSpec();
        m_size = size;
    }

    private static TableTransform resolveFullTableTransform(final TableTransform fragment,
        final ReferenceTable[] referenceTables) {
        return fragment.reSource(//
            Stream.of(referenceTables)//
                .collect(//
                    Collectors.toMap(//
                        ReferenceTable::getId, //
                        t -> t.getVirtualTable().getProducingTransform()//
                    )//
                )//
        );
    }

    /**
     * @return the schema of the table
     */
    public ColumnarValueSchema getSchema() {
        return m_schema;
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        m_schema.save(settings.addNodeSettings(CFG_SCHEMA));
        settings.addIntArray(CFG_REF_TABLES, //
            Stream.of(m_refTables)//
                .mapToInt(BufferedDataTable::getBufferedTableId)//
                .toArray()//
        );
        settings.addLong(CFG_SIZE, m_size);
        settings.addStringArray(CFG_REF_TABLE_IDS, //
            Stream.of(m_referenceTables)//
                .map(ReferenceTable::getId)//
                .map(UUID::toString)//
                .toArray(String[]::new)//
        );
        TableTransformNodeSettingsPersistor.save(m_tableTransformFragment,
            settings.addNodeSettings(CFG_VIRTUAL_TABLE_FRAGMENT));
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
        try {
            m_openCursors.close();
        } catch (IOException ex) {
            LOGGER.debug("Exception while closing open cursors.", ex);
        }
        clearOutputCache();
        for (var referenceTable : m_referenceTables) {
            referenceTable.clearIfNecessary();
        }
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
        dataRepository.addTable(m_tableId, this);
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

    @Override
    public CloseableRowIterator iterator() {
        return new ColumnarRowIterator(cursor());
    }

    @SuppressWarnings("resource")
    @Override
    public CloseableRowIterator iteratorWithFilter(final TableFilter filter) {
        if (TableFilterUtils.hasFilter(filter)) {
            var filteredCursor = cursor(filter);
            return filter.getMaterializeColumnIndices()
                .map(m -> FilteredColumnarRowIteratorFactory.create(filteredCursor, m))
                .orElseGet(() -> new ColumnarRowIterator(filteredCursor));
        } else {
            return iterator();
        }
    }

    @Override
    public CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
        return iteratorWithFilter(filter);
    }

    @Override
    public RowCursor cursor() {
        return m_openCursors
            .createTrackedCursor(() -> VirtualTableUtils.createColumnarRowCursor(m_schema, getOutput().createCursor()));
    }

    synchronized RowAccessible getOutput() {
        if (m_cachedOutputs == null) {
            m_cachedOutputs = runComputationGraph();
        }
        return m_cachedOutputs.get(0);
    }

    boolean hasCachedOutput() {//NOSONAR
        return m_cachedOutputs != null;
    }

    private List<RowAccessible> runComputationGraph() {
        final VirtualTableExecutor exec = new GraphVirtualTableExecutor(m_resolvedTableTransform);
        final Map<UUID, RowAccessible> sources = collectSources();
        return exec.execute(sources);
    }

    public Map<UUID, RowAccessible> collectSources() {
        final Map<UUID, RowAccessible> sources = new HashMap<>();
        for (var i = 0; i < m_referenceTables.length; i++) {
            sources.putAll(m_referenceTables[i].getSources());
        }
        return sources;
    }

    @Override
    public RowCursor cursor(final TableFilter filter) {
        if (TableFilterUtils.hasFilter(filter)) {
            return m_openCursors.createTrackedCursor(() -> createdFilteredRowCursor(filter));
        } else {
            // cursor() already does the tracking for us
            return cursor();
        }
    }

    @SuppressWarnings("resource") // we track both the accessibles and the cursor using CloseableTracker
    private RowCursor createdFilteredRowCursor(final TableFilter filter) {
        var table = createFilteredVirtualTable(filter);
        final VirtualTableExecutor exec = new GraphVirtualTableExecutor(table.getProducingTransform());
        final List<RowAccessible> accessibles = exec.execute(collectSources());
        final Cursor<ReadAccessRow> physicalCursor = accessibles.get(0).createCursor();
        var cursor = TableFilterUtils.createColumnSelection(filter, m_schema.numColumns())//
            .map(s -> VirtualTableUtils.createTableFilterRowCursor(m_schema, physicalCursor, s))//
            .orElseGet(() -> VirtualTableUtils.createColumnarRowCursor(m_schema, physicalCursor));
        return new SourceClosingRowCursor(cursor, accessibles.toArray(Closeable[]::new));
    }

    private ColumnarVirtualTable createFilteredVirtualTable(final TableFilter filter) {
        var table = getVirtualTable();

        if (TableFilterUtils.definesRowRange(filter)) {
            table = table.slice(TableFilterUtils.extractFromIndex(filter),
                TableFilterUtils.extractToIndex(filter, m_size) + 1);
        }

        if (TableFilterUtils.definesColumnFilter(filter)) {
            table = table.selectColumns(TableFilterUtils.extractPhysicalColumnIndices(filter, m_schema.numColumns()));
        }
        return table;
    }

    public ColumnarVirtualTable getVirtualTable() {
        return new ColumnarVirtualTable(m_resolvedTableTransform, m_schema);
    }


    private static final class SourceClosingRowCursor implements RowCursor {

        private final RowCursor m_delegate;

        private final Closeable[] m_sources;

        SourceClosingRowCursor(final RowCursor cursor, final Closeable[] sources) {
            m_sources = sources;
            m_delegate = cursor;
        }

        @Override
        public RowRead forward() {
            return m_delegate.forward();
        }

        @Override
        public boolean canForward() {
            return m_delegate.canForward();
        }

        @Override
        public int getNumColumns() {
            return m_delegate.getNumColumns();
        }

        @Override
        public void close() {
            m_delegate.close();
            for (var source : m_sources) {
                try {
                    source.close();
                } catch (IOException ex) {
                    LOGGER.debug("Failed to close source RowAccessibles.", ex);
                }
            }
        }

    }
}
