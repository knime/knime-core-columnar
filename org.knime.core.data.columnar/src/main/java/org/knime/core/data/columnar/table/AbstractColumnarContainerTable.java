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
package org.knime.core.data.columnar.table;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.columnar.table.ResourceLeakDetector.ResourceWithRelease;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
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
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.LookaheadRowAccessible;

/**
 * Implementation of an {@link ExtensionTable}. This table is managed by the KNIME framework and allows to access data
 * from within a {@link BatchStore}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractColumnarContainerTable extends ExtensionTable implements ColumnarContainerTable {

    protected static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractColumnarContainerTable.class);

    private static final String CFG_FACTORY_TYPE = "columnstore_factory_type";

    private static final String CFG_TABLE_SIZE = "table_size";

    private static ColumnStoreFactory createInstance(final String type) throws InvalidSettingsException {
        try {
            ColumnStoreFactory factory = ColumnStoreFactoryRegistry.getOrCreateInstance().getFactorySingleton();
            if (!Objects.equals(factory.getClass().getName(), type)) {
                throw new InvalidSettingsException(
                    String.format("Class of column store factory not as expected (installed: %s, requested: %s)",
                        factory.getClass().getName(), type));
            }
            return factory;
        } catch (InvalidSettingsException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidSettingsException("Unable to instantiate object of type: " + type, e);
        }
    }

    private final long m_tableId;

    private final ColumnarRowReadTable m_columnarTable;

    private final ColumnarContainerRowAccessible m_rowAccessibleView = new ColumnarContainerRowAccessible();

    private final CursorTracker<CloseableRowIterator> m_iteratorTracker = CursorTracker.createRowIteratorTracker();

    // effectively final. This is a safety net for the case that a table gets GC'ed without being cleared by the AP
    private Finalizer m_tableCloser;

    @SuppressWarnings("resource")
    AbstractColumnarContainerTable(final LoadContext context) throws InvalidSettingsException {
        final NodeSettingsRO settings = context.getSettings();
        m_tableId = -1;
        final var size = settings.getLong(CFG_TABLE_SIZE);
        final ColumnStoreFactory factory = createInstance(settings.getString(CFG_FACTORY_TYPE));
        final var dataPath = context.getDataFileRef().getFile().toPath();
        final BatchReadStore readStore = factory.createReadStore(dataPath);
        var schema = ColumnarValueSchemaUtils.load(readStore.getSchema(), context);
        m_columnarTable = new ColumnarRowReadTable(schema, factory, readStore, size);
    }

    AbstractColumnarContainerTable(final int tableId, final ColumnarRowReadTable columnarTable) {
        m_tableId = tableId;
        m_columnarTable = columnarTable;
    }

    void initStoreCloser() {
        final var tableRelease = new ResourceWithRelease(m_columnarTable);
        m_tableCloser = ResourceLeakDetector.getInstance().createFinalizer(this, tableRelease);
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        settings.addLong(CFG_TABLE_SIZE, m_columnarTable.size());
        settings.addString(CFG_FACTORY_TYPE, m_columnarTable.getStoreFactory().getClass().getName());
        m_columnarTable.getSchema().save(settings);
    }

    @Override
    public DataTableSpec getDataTableSpec() {
        return m_columnarTable.getSchema().getSourceSpec();
    }

    @Override
    public ColumnarValueSchema getSchema() {
        return m_columnarTable.getSchema();
    }

    @Override
    public int getTableId() {
        return (int)m_tableId;
    }

    @Deprecated
    @Override
    public int getRowCount() { // NOSONAR
        return KnowsRowCountTable.checkRowCount(m_columnarTable.size());
    }

    @Override
    public void putIntoTableRepository(final WorkflowDataRepository dataRepository) {
        // only relevant in case of newly created tables
        dataRepository.addTable((int)m_tableId, this);
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
    public long size() {
        return m_columnarTable.size();
    }

    @Override
    public void clear() {
        m_tableCloser.close();
        try {
            m_iteratorTracker.close();
            m_columnarTable.close();
        } catch (final IOException e) {
            LOGGER.error(String.format("Exception while clearing ContainerTable: %s", e.getMessage()), e);
        }
    }

    @Override
    public BufferedDataTable[] getReferenceTables() {
        return new BufferedDataTable[0];
    }

    @Override
    public ColumnarBatchReadStore getStore() {
        return m_columnarTable.getStore();
    }

    @Override
    public final void ensureOpen() {
        // NB: We directly read from workspace and don't copy data to temp for reading. Therefore: Noop.
    }

    @Override
    public RowCursor cursor() {
        return m_columnarTable.createRowCursor();
    }

    @Override
    public RowCursor cursor(final TableFilter filter) {
        return m_columnarTable.createRowCursor(filter);
    }

    @Override
    public final CloseableRowIterator iterator() {
        return m_iteratorTracker.createTrackedCursor(
            () -> (new PrefetchingRowIterator(new ColumnarRowIterator(m_columnarTable.createUntrackedRowCursor()))));
    }

    @Override
    public CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
        return m_iteratorTracker.createTrackedCursor(() -> iteratorWithFilterInternal(filter));
    }

    @SuppressWarnings("resource") // the calling method tracks the iterator
    private final CloseableRowIterator iteratorWithFilterInternal(final TableFilter filter) {
        final Optional<Set<Integer>> materializeColumnIndices = filter.getMaterializeColumnIndices();
        final var iterator = materializeColumnIndices.isPresent()
            ? FilteredColumnarRowIteratorFactory.create(m_columnarTable.createUntrackedRowCursor(filter),
                materializeColumnIndices.get())
            : new ColumnarRowIterator(m_columnarTable.createUntrackedRowCursor(filter));
        return new PrefetchingRowIterator(iterator);
    }

    /**
     * @return a view of this table as {@link LookaheadRowAccessible}
     */
    public LookaheadRowAccessible asRowAccessible() {
        return m_rowAccessibleView;
    }

    private final class ColumnarContainerRowAccessible implements LookaheadRowAccessible {

        @Override
        public void close() throws IOException {
            // the life-cycle of this view is bound to the life-cycle of the outer instance
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_columnarTable.getSchema();
        }

        @Override
        public long size() {
            return m_columnarTable.size();
        }

        @Override
        public LookaheadCursor<ReadAccessRow> createCursor() {
            return m_columnarTable.createCursor();
        }

        @Override
        public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
            return m_columnarTable.createCursor(selection);
        }
    }
}
