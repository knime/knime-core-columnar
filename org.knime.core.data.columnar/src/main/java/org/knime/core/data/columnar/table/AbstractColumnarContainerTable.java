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
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.DefaultColumnarBatchReadStore.ColumnarBatchReadStoreBuilder;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.columnar.table.ResourceLeakDetector.ResourceWithRelease;
import org.knime.core.data.columnar.table.virtual.closeable.CloseableTracker;
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
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Implementation of an {@link ExtensionTable}. This table is managed by the KNIME framework and allows to access data
 * from within a {@link BatchStore}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractColumnarContainerTable extends ExtensionTable implements ColumnarContainerTable {

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

    protected final ColumnarRowReadTable m_columnarTable;

    private final Set<Finalizer> m_openCursorFinalizers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // effectively final
    private Finalizer m_tableCloser;

    @SuppressWarnings("resource")
    AbstractColumnarContainerTable(final LoadContext context) throws InvalidSettingsException {
        final NodeSettingsRO settings = context.getSettings();
        m_tableId = -1;
        final var size = settings.getLong(CFG_TABLE_SIZE);
        final ColumnStoreFactory factory = createInstance(settings.getString(CFG_FACTORY_TYPE));
        final var dataPath = context.getDataFileRef().getFile().toPath();
        final ColumnarBatchReadStore readStore =
            new ColumnarBatchReadStoreBuilder(factory.createReadStore(dataPath)) //
                .enableDictEncoding(true) //
                .useColumnDataCache(ColumnarPreferenceUtils.getColumnDataCache()) //
                .useHeapCache(ColumnarPreferenceUtils.getHeapCache()) //
                .build();
        var schema = ColumnarValueSchemaUtils.load(readStore.getSchema(), context);
        m_columnarTable = new ColumnarRowReadTable(schema, factory, readStore, dataPath, size);
    }

    AbstractColumnarContainerTable(final int tableId, final ColumnarRowReadTable columnarTable) {
        m_tableId = tableId;
        m_columnarTable = columnarTable;
    }

    void initStoreCloser() {
        final var readersRelease = new ResourceWithRelease(m_openCursorFinalizers,
            finalizers -> finalizers.forEach(Finalizer::releaseResourcesAndLogOutput));
        final var tableRelease = new ResourceWithRelease(m_columnarTable);
        m_tableCloser = ResourceLeakDetector.getInstance().createFinalizer(this, readersRelease, tableRelease);
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

    ColumnarValueSchema getSchema() {
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
        for (final Finalizer closer : m_openCursorFinalizers) {
            closer.releaseResourcesAndLogOutput();
        }
        m_openCursorFinalizers.clear();
        try {
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
        return m_columnarTable.createCursor(m_openCursorFinalizers);
    }

    @Override
    public RowCursor cursor(final TableFilter filter) {
        return m_columnarTable.createCursor(m_openCursorFinalizers, filter);
    }

    @SuppressWarnings("resource") // Cursor will be closed along with iterator.
    @Override
    public final CloseableRowIterator iterator() {
        return new ColumnarRowIterator(cursor());
    }

    @Override
    public final CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
        final Optional<Set<Integer>> materializeColumnIndices = filter.getMaterializeColumnIndices();
        @SuppressWarnings("resource") // Cursor will be closed along with iterator.
        final var iterator = materializeColumnIndices.isPresent()
            ? FilteredColumnarRowIteratorFactory.create(cursor(filter), materializeColumnIndices.get())
            : new ColumnarRowIterator(cursor(filter));
        return iterator;
    }

    RowAccessible asRowAccessible() {
        return new ColumnarContainerRowAccessible();
    }

    private final class ColumnarContainerRowAccessible implements RowAccessible {

        private final CloseableTracker<Cursor<ReadAccessRow>, IOException> m_trackedCursors =
            new CloseableTracker<>(IOException.class);

        @Override
        public void close() throws IOException {
            m_trackedCursors.close();
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_columnarTable.getSchema();
        }

        @SuppressWarnings("resource") // the purpose of the tracker is memory leaks
        @Override
        public Cursor<ReadAccessRow> createCursor() {
            return m_trackedCursors.track(m_columnarTable.createCursor());
        }
    }
}
