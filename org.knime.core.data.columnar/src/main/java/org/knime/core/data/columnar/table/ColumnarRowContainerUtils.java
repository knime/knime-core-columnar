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
 *   Nov 8, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.table;

import java.io.IOException;

import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.data.columnar.ColumnStoreFactoryRegistry;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.node.ExecutionContext;

/**
 * Utility class to create {@link DataContainerDelegate}s and {@link RowContainer}s.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class ColumnarRowContainerUtils {

    private ColumnarRowContainerUtils() {
    }

    static RowContainer create(final int tableId, final ColumnStoreFactory storeFactory,
        final ColumnarValueSchema schema, final ColumnarRowContainerSettings settings) throws Exception {
        return createInternal(null, tableId, storeFactory, schema, settings);
    }

    @SuppressWarnings({"javadoc"})
    public static RowContainer create(final ExecutionContext context, final int tableId,
        final ColumnarValueSchema schema, final ColumnarRowContainerSettings settings) throws Exception {
        return createInternal(context, tableId, schema, settings);
    }

    @SuppressWarnings({"javadoc", "resource"})
    public static DataContainerDelegate create(final int tableId, final ColumnarValueSchema schema,
        final ColumnarRowContainerSettings config) throws Exception {
        return new ColumnarDataContainerDelegate(schema.getSourceSpec(), createInternal(null, tableId, schema, config));
    }

    private static ColumnarRowContainer createInternal(final ExecutionContext context, final int tableId,
        final ColumnarValueSchema schema, final ColumnarRowContainerSettings config) throws Exception {
        return createInternal(context, tableId, ColumnStoreFactoryRegistry.getOrCreateInstance().getFactorySingleton(),
            schema, config);
    }

    private static ColumnarRowContainer createInternal(final ExecutionContext context, final int tableId,
        final ColumnStoreFactory storeFactory, final ColumnarValueSchema schema,
        final ColumnarRowContainerSettings config) throws IOException {
        return ColumnarRowContainer.create(context, tableId, schema, storeFactory, config);
    }
}
