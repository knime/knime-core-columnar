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

import org.knime.core.columnar.cursor.ColumnarCursorFactory;
import org.knime.core.columnar.data.dictencoding.DictEncodedBatchReadable;
import org.knime.core.columnar.filter.DefaultBatchRange;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.columnar.table.ResourceLeakDetector.ResourceWithRelease;
import org.knime.core.data.columnar.table.virtual.CloseableTrackingSupplier;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
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
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Implementation of an {@link ExtensionTable}. This table is managed by the KNIME framework and allows to access data
 * from within a {@link BatchStore}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractColumnarContainerTable extends ExtensionTable {

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

    private final ColumnStoreFactory m_factory;

    private final ColumnarValueSchema m_schema;

    private final long m_tableId;

    private final long m_size;

    private final Set<Finalizer> m_openCursorFinalizers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // TODO AP-17236 would allow to implement a BatchReadStoreRowAccessible that lives in org.knime.core.columnar
    // this class would then simply wrap one of these
    private final BatchReadStore m_readStore;


    // effectively final
    private Finalizer m_storeCloser;

    @SuppressWarnings("resource")
    AbstractColumnarContainerTable(final LoadContext context) throws InvalidSettingsException {
        final NodeSettingsRO settings = context.getSettings();
        m_tableId = -1;
        m_size = settings.getLong(CFG_TABLE_SIZE);
        m_factory = createInstance(settings.getString(CFG_FACTORY_TYPE));
        m_schema = ColumnarValueSchemaUtils
            .create(ValueSchema.Serializer.load(context.getTableSpec(), context.getDataRepository(), settings));

        final BatchReadStore store = m_factory.createReadStore(m_schema, context.getDataFileRef().getFile().toPath());
        final CachedBatchReadable cached = new CachedBatchReadable(store);
        final DictEncodedBatchReadable dictEncoded = new DictEncodedBatchReadable(cached);

        m_readStore = new WrappedBatchReadStore(dictEncoded, store.numBatches(), store.batchLength());
    }

    AbstractColumnarContainerTable(final int tableId, final ColumnStoreFactory factory,
        final ColumnarValueSchema schema, final BatchReadStore store, final long size) {
        m_tableId = tableId;
        m_factory = factory;
        m_schema = schema;
        m_size = size;
        m_readStore = store;
    }

    void initStoreCloser() {
        final ResourceWithRelease readersRelease = new ResourceWithRelease(m_openCursorFinalizers,
            finalizers -> finalizers.forEach(Finalizer::releaseResourcesAndLogOutput));
        final ResourceWithRelease storeRelease = new ResourceWithRelease(m_readStore);
        m_storeCloser = ResourceLeakDetector.getInstance().createFinalizer(this, readersRelease, storeRelease);
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        settings.addLong(CFG_TABLE_SIZE, m_size);
        settings.addString(CFG_FACTORY_TYPE, m_factory.getClass().getName());
        m_schema.save(settings);
    }

    @Override
    public DataTableSpec getDataTableSpec() {
        return m_schema.getSourceSpec();
    }

    ColumnarValueSchema getSchema() {
        return m_schema;
    }

    @Override
    public int getTableId() {
        return (int)m_tableId;
    }

    @Deprecated
    @Override
    public int getRowCount() { // NOSONAR
        return KnowsRowCountTable.checkRowCount(m_size);
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
        return m_size;
    }

    @Override
    public void clear() {
        m_storeCloser.close();
        for (final Finalizer closer : m_openCursorFinalizers) {
            closer.releaseResourcesAndLogOutput();
        }
        m_openCursorFinalizers.clear();
        try {
            m_readStore.close();
        } catch (final IOException e) {
            LOGGER.error(String.format("Exception while clearing ContainerTable: %s", e.getMessage()), e);
        }
    }

    @Override
    public BufferedDataTable[] getReferenceTables() {
        return new BufferedDataTable[0];
    }

    @Override
    public final void ensureOpen() {
        // NB: We directly read from workspace and don't copy data to temp for reading. Therefore: Noop.
    }

    @Override
    public RowCursor cursor() {
        try {
            return ColumnarRowCursorFactory.create(m_readStore, m_schema, m_size, m_openCursorFinalizers);
        } catch (IOException e) {
            throw new IllegalStateException("Exception while creating ColumnarRowCursor.", e);
        }
    }

    @Override
    public RowCursor cursor(final TableFilter filter) {
        try {
            if (filter != null) {
                filter.validate(getDataTableSpec(), m_size);
                //  for some reason we don't do validation of the 'from index'
                final Optional<Long> fromRowIndexOpt = filter.getFromRowIndex();
                if (fromRowIndexOpt.isPresent()) {
                    final long fromRowIndex = fromRowIndexOpt.get();
                    if (fromRowIndex >= m_size) { // NOSONAR
                        throw new IndexOutOfBoundsException(
                            String.format("From row index %d too large for table of size %d.", fromRowIndex, m_size));
                    }
                }
            }
            return ColumnarRowCursorFactory.create(m_readStore, m_schema, m_size, m_openCursorFinalizers, filter);
        } catch (IOException e) {
            throw new IllegalStateException("Exception while creating ColumnarRowCursor.", e);
        }
    }

    @Override
    @SuppressWarnings("resource")
    public final CloseableRowIterator iterator() {
        return new ColumnarRowIterator(cursor());
    }

    @Override
    @SuppressWarnings("resource")
    public final CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
        final Optional<Set<Integer>> materializeColumnIndices = filter.getMaterializeColumnIndices();
        return materializeColumnIndices.isPresent()
            ? FilteredColumnarRowIteratorFactory.create(cursor(filter), materializeColumnIndices.get())
            : new ColumnarRowIterator(cursor(filter));
    }

    RowAccessible asRowAccessible() {
        return new ColumnarContainerRowAccessible();
    }

    private final class ColumnarContainerRowAccessible implements RowAccessible {

        private final CloseableTrackingSupplier<Cursor<ReadAccessRow>> m_cursors =
            new CloseableTrackingSupplier<>(this::createCursorInternal);

        @Override
        public void close() throws IOException {
            m_cursors.close();
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_schema;
        }

        @Override
        public Cursor<ReadAccessRow> createCursor() {
            return m_cursors.get();
        }

        private LookaheadCursor<ReadAccessRow> createCursorInternal() {
            final int lastIndexInLastBatch = (int)((m_size - 1) % m_readStore.batchLength());
            final var batchRange = new DefaultBatchRange(0, 0, m_readStore.numBatches() - 1, lastIndexInLastBatch);
            return ColumnarCursorFactory.create(m_readStore, new DefaultColumnSelection(m_schema.numColumns()),
                batchRange);
        }
    }

}
