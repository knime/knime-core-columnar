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
 *   Jul 20, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.v2.ReadAccessRowRead;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.schema.DataTableValueSchema;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.Cursors;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;

/**
 * Provides utility functions for dealing with the conversion between the physical (accesses) and logical layer (values)
 * of fast tables.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class VirtualTableUtils {

    /**
     * Creates a {@link RowCursor} that is backed by the provided {@link Cursor}.
     *
     * @param schema of the table
     * @param cursor to wrap
     * @return {@link RowCursor} that is backed by the provided {@link Cursor}
     */
    @SuppressWarnings("resource") // the lookaheadCursor is managed by the returned RowCursor
    public static RowCursor createColumnarRowCursor(final ValueSchema schema,
        final Cursor<ReadAccessRow> cursor) {
        final var lookaheadCursor = Cursors.toLookahead(schema, cursor);
        final var accessRow = new ReadAccessRowRead(schema, lookaheadCursor.access());
        return new ColumnarRowCursor(accessRow, lookaheadCursor);
    }

    /**
     * Creates a {@link RowAccessible} that is backed by a {@link BufferedDataTable}.
     *
     * @param schema corresponding to the provided table
     * @param table
     * @return a {@link RowAccessible} that is backed by the provided table
     */
    public static RowAccessible createRowAccessible(final ValueSchema schema, final BufferedDataTable table) {
        CheckUtils.checkArgument(getDataTableSpec(schema).equals(table.getDataTableSpec()),
            "The schema must match the table.");
        return new BufferedDataTableRowAccessible(table, schema);
    }

    private static DataTableSpec getDataTableSpec(final ValueSchema schema) {
        if (schema instanceof DataTableValueSchema s) {
            return s.getSourceSpec();
        } else {
            return ValueSchemaUtils.createDataTableSpec(schema);
        }
    }

    private VirtualTableUtils() {
        // static utility class
    }
}
