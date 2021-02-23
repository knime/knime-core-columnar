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
 *   Nov 9, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.columnar.domain.DefaultDomainStoreConfig;
import org.knime.core.data.columnar.domain.DomainColumnStore;
import org.knime.core.data.columnar.domain.DuplicateCheckColumnStore;
import org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.ResourceLeakDetector.Finalizer;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExtensionTable;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.DuplicateChecker;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class ColumnarRowContainer implements RowContainer {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ColumnarRowContainer.class);

    static ColumnarRowContainer create(final ExecutionContext context, final int id, final ColumnarValueSchema schema,
        final ColumnStoreFactory storeFactory, final ColumnarRowContainerSettings settings) throws IOException {
        final ColumnarRowContainer container = new ColumnarRowContainer(context, id, schema, storeFactory, settings);
        container.m_finalizer =
            ResourceLeakDetector.getInstance().createFinalizer(container, container.m_delegate, container.m_store);
        return container;
    }

    private final ColumnarRowWriteCursor m_delegate;

    private final ExecutionContext m_context;

    private final int m_id;

    private final ColumnStoreFactory m_storeFactory;

    private final DomainColumnStore m_store;

    private final ColumnarValueSchema m_schema;

    private Finalizer m_finalizer;

    private ExtensionTable m_table;

    @SuppressWarnings("resource")
    private ColumnarRowContainer(final ExecutionContext context, final int id, final ColumnarValueSchema schema,
        final ColumnStoreFactory storeFactory, final ColumnarRowContainerSettings settings) throws IOException {
        m_id = id;
        m_schema = schema;
        m_context = context;
        m_storeFactory = storeFactory;

        ColumnStore wrapped =
            ColumnarPreferenceUtils.wrap(m_storeFactory.createStore(schema, DataContainer.createTempFile(".knable")));
        if (settings.isCheckDuplicateRowKeys()) {
            wrapped = new DuplicateCheckColumnStore(wrapped, new DuplicateChecker(),
                ColumnarPreferenceUtils.getDuplicateCheckExecutor());
        }
        m_store = new DomainColumnStore(wrapped, new DefaultDomainStoreConfig(schema,
            settings.getMaxPossibleNominalDomainValues(), settings.isInitializeDomains()),
            ColumnarPreferenceUtils.getDomainCalcExecutor());

        m_delegate = new ColumnarRowWriteCursor(m_store, schema.getWriteValueFactories());
    }

    @Override
    public ColumnarRowWriteCursor createCursor() {
        return m_delegate;
    }

    @SuppressWarnings("resource")
    @Override
    public BufferedDataTable finish() {
        if (m_context == null) {
            throw new IllegalStateException(
                "ColumnarRowContainer has not been initialized with ExecutionContext. Implementation error.");
        }
        return finishInternal().create(m_context);
    }

    @Override
    public final void close() {
        // in case m_table was not created, we have to destroy the store. otherwise the
        // m_table has a handle on the store and therefore the store shouldn't be destroyed.
        try {
            if (m_table == null) {
                m_finalizer.close();
                m_delegate.close();
                // closing the store includes closing the writer
                // (but will make sure duplicate checks and domain calculations are halted)
                m_store.close();
            }
        } catch (final IOException e) {
            LOGGER.error("Exception while closing store.", e);
        }
    }

    ExtensionTable finishInternal() {
        if (m_table == null) {
            m_finalizer.close();
            m_delegate.finish();

            final Map<Integer, DataColumnDomain> domains = new HashMap<>();
            final Map<Integer, DataColumnMetaData[]> metadata = new HashMap<>();
            final int numColumns = m_schema.numColumns();
            for (int i = 1; i < numColumns; i++) {
                domains.put(i, m_store.getDomain(i));
                metadata.put(i, m_store.getMetadata(i));
            }

            m_table = UnsavedColumnarContainerTable.create(m_id, m_storeFactory,
                ColumnarValueSchemaUtils.updateSource(m_schema, domains, metadata), m_store, m_delegate.size());
        }
        return m_table;
    }

    /**
     * Only to be used by ColumnarDataContainerDelegate#setMaxPossibleValues(int) for backward compatibility reasons.
     *
     * @param maxPossibleValues the maximum number of values for a nominal domain.
     *
     * @apiNote No API.
     */
    void setMaxPossibleValues(final int maxPossibleValues) {
        m_store.setMaxPossibleValues(maxPossibleValues);
    }

}
