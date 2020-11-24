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

import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.columnar.table.ResourceLeakDetector.ResourceWithRelease;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.ValueSchema;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.ExtensionTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.WorkflowDataRepository;

/**
 * Abstract implementation of an {@link ExtensionTable}. This table is managed by the KNIME framework and allows access
 * data from within a {@link ColumnStore}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
abstract class AbstractColumnarContainerTable extends ExtensionTable {

    static final String CFG_FACTORY_TYPE = "columnstore_factory_type";

    private static final String CFG_TABLE_SIZE = "table_size";

    private final ColumnStoreFactory m_factory;

    private final ColumnarValueSchema m_schema;

    private final long m_tableId;

    private final long m_size;

    private final Set<Finalizer> m_openCursorFinalizers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ColumnReadStore m_store;

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
        m_store = ColumnarPreferenceUtils.wrap(m_factory.createReadStore(m_schema, context.getDataFileRef().getFile()));
    }

    void initStoreCloser() {
        final ResourceWithRelease readersRelease = new ResourceWithRelease(m_openCursorFinalizers,
            finalizers -> finalizers.forEach(Finalizer::releaseResourcesAndLogOutput));
        final ResourceWithRelease storeRelease = new ResourceWithRelease(m_store);
        m_storeCloser = ResourceLeakDetector.getInstance().createFinalizer(this, readersRelease, storeRelease);
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        settings.addLong(CFG_TABLE_SIZE, m_size);
        settings.addString(CFG_FACTORY_TYPE, m_factory.getClass().getName());
        ValueSchema.Serializer.save(m_schema.getSourceSchema(), settings);
    }

    public AbstractColumnarContainerTable(final int tableId, final ColumnStoreFactory factory,
        final ColumnarValueSchema schema, final ColumnReadStore store, final long size) {
        m_tableId = tableId;
        m_factory = factory;
        m_schema = schema;
        m_store = store;
        m_size = size;
    }

    @Override
    public DataTableSpec getDataTableSpec() {
        return m_schema.getSourceSpec();
    }

    @Override
    public int getTableId() {
        return (int)m_tableId;
    }

    @Deprecated
    @Override
    public int getRowCount() {
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
        dataRepository.removeTable((int)m_tableId);
        return true;
    }

    @Override
    public long size() {
        return m_size;
    }

    @Override
    public void clear() {
        if (m_storeCloser != null) {
            m_storeCloser.close();
        }
        for (final Finalizer closer : m_openCursorFinalizers) {
            closer.releaseResourcesAndLogOutput();
        }
        m_openCursorFinalizers.clear();
        try {
            m_store.close();
        } catch (final IOException e) {
            throw new IllegalStateException("Exception while clearing ContainerTable.", e);
        }
    }

    public BufferedDataTable getBufferedDataTable(final ExecutionContext context) {
        return create(context);
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
        return ColumnarRowCursor.create(m_store, m_schema, 0, m_size - 1, m_openCursorFinalizers);
    }

    @Override
    public RowCursor cursor(final TableFilter filter) {
        final long fromRowIndex = filter.getFromRowIndex().orElse(0l);
        final long toRowIndex = filter.getToRowIndex().orElse(m_size - 1);

        final Optional<Set<Integer>> colIndicesOpt = filter.getMaterializeColumnIndices();
        if (colIndicesOpt.isPresent()) {
            return ColumnarRowCursor.create(m_store, m_schema, fromRowIndex, toRowIndex, m_openCursorFinalizers,
                toSortedIntArray(colIndicesOpt.get()));
        } else {
            return ColumnarRowCursor.create(m_store, m_schema, fromRowIndex, toRowIndex, m_openCursorFinalizers);
        }
    }

    @Override
    @SuppressWarnings("resource")
    public final CloseableRowIterator iterator() {
        return new ColumnarRowIterator(
            ColumnarRowCursor.create(m_store, m_schema, 0, m_size - 1, m_openCursorFinalizers));
    }

    @Override
    @SuppressWarnings("resource")
    public final CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
        final long fromRowIndex = filter.getFromRowIndex().orElse(0l);
        final long toRowIndex = filter.getToRowIndex().orElse(m_size - 1);

        final Optional<Set<Integer>> colIndicesOpt = filter.getMaterializeColumnIndices();
        if (colIndicesOpt.isPresent()) {
            final int[] selection = toSortedIntArray(colIndicesOpt.get());
            return FilteredColumnarRowIterator.create(ColumnarRowCursor.create(m_store, m_schema, fromRowIndex,
                toRowIndex, m_openCursorFinalizers, selection), selection);
        } else {
            return new ColumnarRowIterator(
                ColumnarRowCursor.create(m_store, m_schema, fromRowIndex, toRowIndex, m_openCursorFinalizers));
        }
    }

    // TODO can we avoid this method somehow?
    final ColumnReadStore getStore() {
        return m_store;
    }

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

    private static int[] toSortedIntArray(final Set<Integer> selection) {
        return selection.stream().sorted().mapToInt((i) -> (i)).toArray();
    }
}
