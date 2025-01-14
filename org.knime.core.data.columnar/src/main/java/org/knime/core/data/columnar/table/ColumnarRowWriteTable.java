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
 *   Oct 28, 2021 (marcel): created
 */
package org.knime.core.data.columnar.table;

import java.io.Flushable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.columnar.domain.DefaultDomainWritableConfig;
import org.knime.core.data.columnar.domain.DomainWritable;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.table.DefaultColumnarBatchStore.ColumnarBatchStoreBuilder;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.NodeLogger;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.WriteAccessRow;

/**
 * Standard implementation of a write-only table that understands KNIME's {@link WriteValue logical} data types and is
 * backed by a columnar store. <br>
 * Data written to the table is discarded when closing the table or its cursor unless {@link #finish()} has been called
 * before, transferring the ownership of the data (i.e. the underlying store) to the returned
 * {@link ColumnarRowReadTable}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarRowWriteTable implements RowWriteAccessible {

    static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarRowWriteTable.class);

    private final ValueSchema m_schema;

    private final ColumnStoreFactory m_storeFactory;

    private final ColumnarBatchStore m_store;

    /**
     * Will be {@code null} if {@link ColumnarRowWriteTableSettings#isCalculateDomains()} of the settings object passed
     * to the constructor returns {@code false}.
     */
    private final DomainWritable m_nullableDomainWritable;

    private final ColumnarRowWriteCursor m_writeCursor;

    private final Finalizer m_finalizer;

    /**
     * {@code null} until {@link #finish()} has been called (which does not necessarily have to happen).
     */
    private ColumnarRowReadTable m_nullableFinishedTable;

    /**
     * Creates a new write-only table with the given schema, backed by a columnar store created using the given factory,
     * and configured by the given settings. When creating the store, the table adheres to the user's current
     * {@link ColumnarPreferenceUtils preferences} regarding the columnar table back end.
     *
     * @param schema The schema of the table.
     * @param storeFactory The factory used to create the table's underlying store.
     * @param settings Settings further governing this table's behavior.
     * @throws IOException If creating the underlying temporary file where the table will be stored failed.
     */
    public ColumnarRowWriteTable(final ValueSchema schema, final ColumnStoreFactory storeFactory,
        final ColumnarRowWriteTableSettings settings) throws IOException {
        m_schema = schema;
        m_storeFactory = storeFactory;
        @SuppressWarnings("resource") // Low-level store will be closed along with the built columnar store.
        final var builder = new ColumnarBatchStoreBuilder(m_storeFactory.createStore(m_schema, new TempFileHandle()));
        if (settings.isUseCaching()) {
            builder //
                .useColumnDataCache( //
                    ColumnarPreferenceUtils.getColumnDataCache(), ColumnarPreferenceUtils.getPersistExecutor())
                .useSmallTableCache(ColumnarPreferenceUtils.getSmallTableCache());

            builder.useHeapCache( //
                ColumnarPreferenceUtils.getHeapCache(), ColumnarPreferenceUtils.getPersistExecutor(),
                ColumnarPreferenceUtils.getSerializeExecutor());//

            // NOTE:
            // We do not use the ReadBatchCache for now because it can cause a deadlock on a memory alert.
            // The cache will be useful when we have random-access rows but this is not the case yet.
            // .useReadBatchCache(ColumnarPreferenceUtils.getReadBatchCache())
        }
        if (ColumnarRowWriteTableSettings.useHeapBadger()) {
            builder.useHeapBadger();
        }
        builder.enableDictEncoding(true);
        if (settings.isCalculateDomains()) {
            builder.useDomainCalculation( //
                new DefaultDomainWritableConfig(m_schema, settings.getMaxPossibleNominalDomainValues(),
                    settings.isInitializeDomains()),
                ColumnarPreferenceUtils.getDomainCalcExecutor());
        }
        if (settings.isCheckDuplicateRowKeys()) {
            builder.useDuplicateChecking(ColumnarPreferenceUtils.getDuplicateCheckExecutor());
        }
        m_store = builder.build();
        // Will return null if the builder did not include domain calculation.
        m_nullableDomainWritable = m_store.getDomainWritable();
        m_writeCursor = new ColumnarRowWriteCursor(m_store, m_schema, settings.isForceSynchronousIO() ? m_store : null);

        m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this, m_writeCursor, m_store);
    }

    void flushObjects() throws IOException {
        m_store.flushObjects();
    }

    /**
     * @return This table's write-only cursor.
     * @implNote Currently only a single cursor is supported, i.e., it is always the same cursor instance that is
     *           returned by this method.
     */
    public ColumnarRowWriteCursor createCursor() {
        return m_writeCursor;
    }

    /**
     * @param maxPossibleValues The maximum number of values for a nominal domain.
     * @noreference Only to be used by {@link ColumnarRowContainer#setMaxPossibleValues(int)} for backward compatibility
     *              reasons.
     */
    void setMaxPossibleValues(final int maxPossibleValues) {
        if (m_nullableDomainWritable != null) {
            m_nullableDomainWritable.setMaxPossibleValues(maxPossibleValues);
        } else {
            throw new IllegalStateException("Domains are not allowed to be updated.");
        }
    }

    /**
     * Turns this write-only table into its read-only result. Subsequent attempts to write to this table will fail.
     * Subsequent calls to {@link #close()} will be ignored.
     *
     * @return The finished table.
     */
    public ColumnarRowReadTable finish() {
        if (m_nullableFinishedTable == null) {
            m_finalizer.close();
            m_writeCursor.finish();
            m_writeCursor.close(); // TODO (TP) ColumnarRowWriteCursor.finish() should simultaneously close() !?
            try {
                // Make sure this actually writes the data underneath. This is needed for the test
                // testDeduplicateRowIDsWithSuffix and would cause a "writing after closing writer" error.
                m_store.flush();
            } catch (IOException ex) {
                LOGGER.error("Exception while flushing store.", ex);
                throw new IllegalStateException("Table could not be written to disk.", ex);
            }
            final ValueSchema schema;
            if (m_nullableDomainWritable != null) {
                final Map<Integer, DataColumnDomain> domains = new HashMap<>();
                final Map<Integer, DataColumnMetaData[]> metadata = new HashMap<>();
                final int numColumns = m_schema.numColumns();
                for (int i = 1; i < numColumns; i++) {
                    domains.put(i, m_nullableDomainWritable.getDomain(i));
                    metadata.put(i, m_nullableDomainWritable.getMetadata(i));
                }
                schema = ValueSchemaUtils.updateDataColumnSpecs(m_schema, domains, metadata);
            } else {
                schema = m_schema;
            }

            // Convert to RowReadTable directly, no additional caches needed because we have all of the caches in
            // the hierarchy of wrapped stores already.
            m_nullableFinishedTable = new ColumnarRowReadTable(schema, m_storeFactory, m_store, m_writeCursor.size());
        }
        return m_nullableFinishedTable;
    }

    /**
     * @return a {@link Flushable} whose flush method ensures that everything in the underlying store is written to disk
     */
    public Flushable getStoreFlusher() {
        return m_store;
    }

    /**
     * Closes this table, discarding any data previously written to it unless {@link #finish()} has been called before,
     * in which case this method does nothing.
     */
    @Override
    public void close() {
        // In case the table was not finished, we have to destroy the underlying store. Otherwise, the finished table
        // has a handle to the store and therefore the store must not be destroyed.
        if (m_nullableFinishedTable == null) {
            m_finalizer.close();
            m_writeCursor.close();
            // Closing the store includes closing the writer (but will make sure duplicate checks and domain
            // calculations are halted).
            try {
                m_store.close();
            } catch (final IOException ex) {
                LOGGER.error("Exception while closing store.", ex);
            }
            m_store.getFileHandle().delete();
        }
    }

    // -- implement RowWriteAccessible --

    @Override
    public ValueSchema getSchema() {
        return m_schema;
    }

    @Override
    public WriteCursor<WriteAccessRow> getWriteCursor() {
        return m_writeCursor.getAccessCursor();
    }
}
