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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.columnar.ColumnDataIndex;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.store.ColumnDataFactory;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.domain.DefaultDomainStoreConfig;
import org.knime.core.data.columnar.domain.DomainColumnStore;
import org.knime.core.data.columnar.domain.DomainColumnStore.DomainColumnDataWriter;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.schema.ColumnarWriteValueFactory;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.columnar.table.ResourceLeakDetector.ResourceWithRelease;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.node.ExtensionTable;
import org.knime.core.util.DuplicateKeyException;

/**
 * Columnar implementation of {@link RowWriteCursor}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @since 4.3
 */
public final class ColumnarRowWriteCursor implements RowWriteCursor<ExtensionTable>, ColumnDataIndex {

    private static final String CHUNK_SIZE_PROPERTY = "knime.columnar.chunksize";

    static final int CHUNK_SIZE = Integer.getInteger(CHUNK_SIZE_PROPERTY, 31000);

    private final ColumnStoreFactory m_storeFactory;

    private final DomainColumnStore m_store;

    private final ResourceWithRelease m_storeRelease;

    private final ColumnDataFactory m_columnDataFactory;

    private final DomainColumnDataWriter m_writer;

    private final int m_tableId;

    private final ColumnarValueSchema m_schema;

    private final ColumnarWriteValueFactory<ColumnWriteData>[] m_factories;

    private Finalizer m_finalizer;

    private WriteValue<?>[] m_values;

    private WriteBatch m_currentBatch;

    private ExtensionTable m_table;

    private long m_currentDataMaxIndex;

    private int m_index = 0;

    private long m_size = 0;

    /**
     *
     * @param tableId table id used by KNIME
     * @param schema
     * @param config
     * @return the cursor
     * @throws IOException
     */
    public static ColumnarRowWriteCursor create(final int tableId, final ColumnarValueSchema schema,
        final ColumnarRowWriteCursorSettings config) throws IOException {
        try {
            return create(tableId, ColumnStoreFactoryRegistry.getOrCreateInstance().getFactorySingleton(), schema,
                config);
        } catch (Exception ex) {
            // TODO logging
            throw new IOException("Can't determine column store backend", ex);
        }
    }

    static ColumnarRowWriteCursor create(final int tableId, final ColumnStoreFactory storeFactory,
        final ColumnarValueSchema schema, final ColumnarRowWriteCursorSettings config) throws IOException {
        final ColumnarRowWriteCursor cursor = new ColumnarRowWriteCursor(tableId, storeFactory, schema, config);
        cursor.switchToNextData();
        return cursor;
    }

    // TODO consider moving store creation out of cursor (why should the cursor care?)
    @SuppressWarnings("resource")
    private ColumnarRowWriteCursor(final int tableId, final ColumnStoreFactory storeFactory,
        final ColumnarValueSchema schema, final ColumnarRowWriteCursorSettings config) throws IOException {
        m_tableId = tableId;
        m_schema = schema;
        m_storeFactory = storeFactory;
        m_store = new DomainColumnStore(
            ColumnarPreferenceUtils.wrap(m_storeFactory.createWriteStore(schema,
                DataContainer.createTempFile(".knable"), calculateChunkSize(schema, CHUNK_SIZE))),
            new DefaultDomainStoreConfig(schema, config.getMaxPossibleNominalDomainValues(), config.getRowKeyConfig(),
                config.isInitializeDomains()),
            ColumnarPreferenceUtils.getDomainCalcExecutor());
        m_storeRelease = new ResourceWithRelease(m_store);

        m_columnDataFactory = m_store.getFactory();
        m_writer = m_store.getWriter();

        @SuppressWarnings("unchecked")
        final ColumnarWriteValueFactory<ColumnWriteData>[] factories =
            new ColumnarWriteValueFactory[schema.getNumColumns()];
        for (int i = 0; i < factories.length; i++) {
            factories[i] = m_schema.getWriteValueFactoryAt(i);
        }
        m_factories = factories;
    }

    /**
     * Only to be used by ColumnarDataContainerDelegate#setMaxPossibleValues(int) for backward compatibility reasons.
     *
     * @param maxPossibleValues the maximum number of values for a nominal domain.
     *
     * @apiNote No API.
     */
    public void setMaxPossibleValues(final int maxPossibleValues) {
        m_writer.setMaxPossibleValues(maxPossibleValues);
    }

    @Override
    public void push() {
        if (++m_index > m_currentDataMaxIndex) {
            switchToNextData();
        }
    }

    @Override
    public <W extends WriteValue<?>> W getWriteValue(final int index) {
        // TODO avoid the `+` we can do the index shifting up-front in the
        // constructor
        @SuppressWarnings("unchecked")
        final W value = (W)m_values[index + 1];
        return value;
    }

    @Override
    public void setMissing(final int index) {
        m_currentBatch.get(index + 1).setMissing(m_index);
    }

    @Override
    public ExtensionTable finish() throws IOException {
        if (m_table == null) {
            releaseCurrentData(m_index);
            m_writer.close();

            final Map<Integer, DataColumnDomain> domains = new HashMap<>();
            final Map<Integer, DataColumnMetaData[]> metadata = new HashMap<>();
            final int numColumns = m_schema.getNumColumns();
            for (int i = 1; i < numColumns; i++) {
                domains.put(i, m_store.getDomains(i));
                metadata.put(i, m_store.getDomainMetadata(i));
            }

            m_table = UnsavedColumnarContainerTable.create(m_tableId, m_storeFactory,
                ColumnarValueSchemaUtils.updateSource(m_schema, domains, metadata), m_store, m_size);
        }
        return m_table;
    }

    @Override
    public void close() {
        // in case m_table was not created, we have to destroy the store. otherwise the
        // m_table has a handle on the store and therefore the store shouldn't be destroyed.
        try {
            if (m_table == null) {
                m_finalizer.close();
                if (m_currentBatch != null) {
                    m_currentBatch.release();
                    m_currentBatch = null;
                }
                // closing the store includes closing the writer
                // (but will make sure duplicate checks and domain calculations are halted)
                m_store.close();
            }
        } catch (DuplicateKeyException e) {
            throw new DuplicateKeyException("Encountered duplicate row ID \"" + e.getKey() + "\"", e.getKey());
        } catch (final Exception e) {
            // TODO logging
            // TODO ignore exception?
            throw new IllegalStateException("Exception while closing store.", e);
        }
    }

    @Override
    public int getNumColumns() {
        return m_schema.getNumColumns() - 1;
    }

    @Override
    public int getIndex() {
        return m_index;
    }

    /**
     * @return the schema
     */
    final ColumnarValueSchema getSchema() {
        return m_schema;
    }

    private final void releaseCurrentData(final int numValues) {
        if (m_currentBatch != null) {
            m_finalizer.close();
            final ReadBatch readBatch = m_currentBatch.close(numValues);
            try {
                m_writer.write(readBatch);
            } catch (final IOException e) {
                // TODO logging
                throw new IllegalStateException("Problem occurred when writing column data.", e);
            }
            readBatch.release();
            m_currentBatch = null;
            m_size += numValues;
        }
    }

    private final void switchToNextData() {
        releaseCurrentData(m_index);

        // TODO can we preload data?
        m_currentBatch = m_columnDataFactory.create();
        m_finalizer = ResourceLeakDetector.getInstance().createFinalizer(this,
            new ResourceWithRelease(m_currentBatch, WriteBatch::release), m_storeRelease);
        m_values = create(m_currentBatch);

        m_currentDataMaxIndex = m_currentBatch.capacity() - 1l;
        m_index = 0;
    }

    private WriteValue<?>[] create(final WriteBatch batch) {
        final WriteValue<?>[] values = new WriteValue<?>[m_schema.getNumColumns()];
        for (int i = 0; i < values.length; i++) {
            final ColumnWriteData data = batch.get(i);
            // TODO check if we have to re-create every read value all the time in case
            // this is expensive.
            values[i] = m_factories[i].createWriteValue(data, this);
        }
        return values;
    }

    /*
     * Simple heuristic to take wide-data into account when determining initial batch size.
     *
     * TODO Adaptable chunk-sizes with better size estimates derived from initial batches or historic data will benefit performance.
     */
    private static int calculateChunkSize(final ColumnStoreSchema schema, final int maxChunkSize) {
        // Conservative estimate of 64MB in case many tables are written simultaneously
        final long targetBatchSizeInBytes = 64 * 1024 * 1024;

        /*
         *  32 bytes is pessimistic estimate for primitive types and moderate estimate for object types in KNIME
         */
        final long rowEstimate = Math.max(1, 32 * schema.getNumColumns());

        // TODO select next closest (lower) power of two.
        return (int)Math.max(1, Math.min(targetBatchSizeInBytes / rowEstimate, maxChunkSize));
    }

}
