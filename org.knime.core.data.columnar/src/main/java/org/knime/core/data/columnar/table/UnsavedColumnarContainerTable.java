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

import org.knime.core.columnar.store.ColumnReadStore;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeSettingsWO;

/**
 * ColumnarContainerTable which has not yet been saved, i.e. all data is still in-memory or temporarily persisted in the
 * temp directory.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
class UnsavedColumnarContainerTable extends AbstractColumnarContainerTable {

    private final ColumnStore m_store;

    // effectively final
    private Finalizer<ColumnReadStore> m_storeCloser;

    static UnsavedColumnarContainerTable create(final int tableId, final ColumnStoreFactory factory,
        final ColumnarValueSchema schema, final ColumnStore store, final long size) {
        final UnsavedColumnarContainerTable table =
            new UnsavedColumnarContainerTable(tableId, factory, schema, store, size);
        table.m_storeCloser = Finalizer.create(table, table.m_store);
        return table;
    }

    /**
     * Constructor for creating a {@link UnsavedColumnarContainerTable}.
     *
     * @param tableId the table id used by KNIME
     * @param factory the column store factory which has been used to create the underlying column store.
     * @param schema the columnar value schema. 1-to-1 mapping to ColumnStoreSchema of store.
     * @param store
     * @param size
     *
     *
     */
    /* TODO can we avoid passing factory, schema, store and size as individual parameters? they all have
     *            dependencies to each other. */
    private UnsavedColumnarContainerTable(final int tableId, //
        final ColumnStoreFactory factory, final ColumnarValueSchema schema, final ColumnStore store, final long size) {
        super(tableId, factory, schema, store, size);
        m_store = store;
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        super.saveToFileOverwrite(f, settings, exec);
        m_store.save(f);
    }

    @Override
    public void clear() {
        m_storeCloser.close();
        super.clear();
    }

}