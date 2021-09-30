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
import java.nio.file.Files;
import java.nio.file.Path;

import org.knime.core.columnar.store.BatchStore;
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
 */
public final class UnsavedColumnarContainerTable extends AbstractColumnarContainerTable {

    private final Flushable m_flushable;

    private final Path m_path;

    /**
     * Create an {@link UnsavedColumnarContainerTable} from a given store at a given path
     * @param path Where the table is saved on disk
     * @param tableId The table id
     * @param factory The factory used to create the kind of {@link BatchStore}s used as back end
     * @param schema The schema of this table
     * @param store The {@link ColumnarBatchReadStore} used to read data from
     * @param flushable The {@link Flushable} to flush in case we need to make sure all data is written to disk
     * @param size The number of rows of the table
     * @return the newly created {@link UnsavedColumnarContainerTable}
     */
    public static UnsavedColumnarContainerTable create(final Path path, final int tableId,
        final ColumnStoreFactory factory, final ColumnarValueSchema schema, final ColumnarBatchReadStore store,
        final Flushable flushable, final long size) {
        final UnsavedColumnarContainerTable table =
            new UnsavedColumnarContainerTable(path, tableId, factory, schema, store, flushable, size);
        table.initStoreCloser();
        return table;
    }

    private UnsavedColumnarContainerTable(final Path path, final int tableId, final ColumnStoreFactory factory,
        final ColumnarValueSchema schema, final ColumnarBatchReadStore store, final Flushable flushable, final long size) {
        super(tableId, factory, schema, store, size);
        m_path = path;
        m_flushable = flushable;
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        super.saveToFileOverwrite(f, settings, exec);
        m_flushable.flush();
        Files.copy(m_path, f.toPath());
    }

    @Override
    public void close() {
        clear();
        super.close();
    }

    @Override
    public void clear() {
        super.clear();
        try {
            Files.deleteIfExists(m_path);
        } catch (IOException e) {
            LOGGER.info("Error when deleting file that backed the UnsavedColumnarContainerTable", e);
        }
    }

}
