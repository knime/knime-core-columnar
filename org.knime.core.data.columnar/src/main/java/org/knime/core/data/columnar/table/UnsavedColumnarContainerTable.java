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
 *   22 Feb 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;

import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.v2.schema.DataTableValueSchema;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeSettingsWO;

/**
 * A {@link ColumnarContainerTable} which has not yet been saved, i.e., all data is still in memory or only temporarily
 * persisted on disk.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class UnsavedColumnarContainerTable extends AbstractColumnarContainerTable {

    private final Flushable m_storeFlusher;

    /**
     * Creates an {@link UnsavedColumnarContainerTable} wrapping the given table.
     *
     * @param tableId The table id.
     * @param schema The schema of this table.
     * @param columnarTable The underlying table.
     * @param storeFlusher The {@link Flushable} (e.g. a cache) we need to flush to make sure all data is written to
     *            disk in case the created table is permanently saved to disk. Must not be {@code null} but can be a
     *            no-op.
     * @return The newly created table.
     */
    public static UnsavedColumnarContainerTable create(final int tableId, final DataTableValueSchema schema,
        final ColumnarRowReadTable columnarTable, final Flushable storeFlusher) {
        final var table = new UnsavedColumnarContainerTable(tableId, schema, columnarTable, storeFlusher);
        // TODO: can't we move this to the constructor (or even to the super class' constructor) and simply get rid of
        // the factory methods here?
        table.initStoreCloser();
        return table;
    }

    /**
     * Creates an {@link UnsavedColumnarContainerTable} from the given store.
     *
     * @param tableId The table id.
     * @param factory The factory which created the underlying store.
     * @param schema The schema of the table.
     * @param store The underlying store.
     * @param storeFlusher The {@link Flushable} (e.g. a cache) we need to flush to make sure all data is written to disk
     *            in case the created table is permanently saved to disk. Must not be {@code null} but can be a no-op.
     * @param size The number of rows contained in the table.
     * @return The newly created table.
     */
    @SuppressWarnings("resource") // Columnar table will be closed along with the container table.
    public static UnsavedColumnarContainerTable create(final int tableId, final ColumnStoreFactory factory,
        final DataTableValueSchema schema, final ColumnarBatchReadStore store, final Flushable storeFlusher,
        final long size) {
        final var table = new UnsavedColumnarContainerTable(tableId, schema,
            new ColumnarRowReadTable(schema, factory, store, size), storeFlusher);
        // TODO: can't we move this to the constructor (or even to the super class' constructor) and simply get rid of
        // the factory methods here?
        table.initStoreCloser();
        return table;
    }

    private UnsavedColumnarContainerTable(final int tableId, final DataTableValueSchema schema,
        final ColumnarRowReadTable columnarTable, final Flushable storeFlusher) {
        super(tableId, schema, columnarTable);
        m_storeFlusher = storeFlusher;
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        m_storeFlusher.flush();
        super.saveToFileOverwrite(f, settings, exec);
    }
}
