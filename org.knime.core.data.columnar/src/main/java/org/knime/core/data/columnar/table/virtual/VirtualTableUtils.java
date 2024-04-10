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

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.IntStream;

import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.Cursors;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;

/**
 * Provides utility functions for dealing with the conversion between the physical (accesses) and logical layer (values)
 * of fast tables.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class VirtualTableUtils {

    /**
     * Creates a RowCursor that has been filtered by a {@link TableFilter}.
     *
     * @param unfilteredSchema {@link ColumnarValueSchema} containing all columns (including unselected ones)
     * @param filteredCursor {@link Cursor} that contains only the selected columns and rows
     * @param columnSelection corresponding to the applied {@link TableFilter}
     * @return a {@link RowCursor} with the unfiltered schema that only contains values for the selected columns
     */
    @SuppressWarnings("resource") // the lookaheadCursor is managed by the returned RowCursor
    public static RowCursor createTableFilterRowCursor(final ValueSchema unfilteredSchema,
        final Cursor<ReadAccessRow> filteredCursor, final ColumnSelection columnSelection) {
        final ColumnarSchema filteredSchema = filter(unfilteredSchema, columnSelection);
        final LookaheadCursor<ReadAccessRow> lookaheadCursor = Cursors.toLookahead(filteredSchema, filteredCursor);
        final var rowRead = new FilteredRowRead(unfilteredSchema, lookaheadCursor.access(), columnSelection);
        return new ColumnarRowCursor(rowRead, lookaheadCursor);
    }

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
        final var accessRow = createRowRead(schema, lookaheadCursor.access());
        return new ColumnarRowCursor(accessRow, lookaheadCursor);
    }

    private static ColumnarSchema filter(final ColumnarSchema unfiltered, final ColumnSelection selection) {
        final List<DataSpec> specs = getSelectedIndexStream(selection)
            .mapToObj(unfiltered::getSpec)//
            .collect(toList());
        final List<DataTraits> traits = getSelectedIndexStream(selection)
                .mapToObj(unfiltered::getTraits)//
                .collect(toList());
        return new DefaultColumnarSchema(specs, traits);
    }

    private static IntStream getSelectedIndexStream(final ColumnSelection selection) {
        return IntStream.range(0, selection.numColumns())//
                .filter(selection::isSelected);
    }

    /**
     * Creates a {@link RowAccessible} that is backed by a {@link BufferedDataTable}.
     *
     * @param schema corresponding to the provided table
     * @param table
     * @return a {@link RowAccessible} that is backed by the provided table
     */
    public static RowAccessible createRowAccessible(final ValueSchema schema, final BufferedDataTable table) {
        CheckUtils.checkArgument(schema.getSourceSpec().equals(table.getDataTableSpec()),
            "The schema must match the table.");
        return new BufferedDataTableRowAccessible(table, schema);
    }

    private static RowRead createRowRead(final ValueSchema schema, final ReadAccessRow accessRow) {
        return new DenseColumnarRowRead(schema, accessRow);
    }

    /**
     * Creates a {@link RowRead} that wraps the provided {@link ReadAccessRow} and respects the provided
     * {@link ColumnSelection}.
     *
     * @param schema of the table
     * @param accessRow to wrap
     * @param columnSelection specifies which columns are included
     * @return a {@link RowRead} that is backed by the provided {@link ReadAccessRow}
     */
    public static RowRead createRowRead(final ValueSchema schema, final ReadAccessRow accessRow,
        final ColumnSelection columnSelection) {
        if (isSparse(columnSelection)) {
            return new SparseColumnarRowRead(schema, accessRow, columnSelection);
        } else {
            return new DenseColumnarRowRead(schema, accessRow);
        }
    }

    private static boolean isSparse(final ColumnSelection columnSelection) {
        return columnSelection != null && IntStream.range(0, columnSelection.numColumns())//
            .anyMatch(i -> !columnSelection.isSelected(i));
    }

    /**
     * Decorates the provided {@link RowAccessible} to be unclosable.
     * <p>
     * The lookahead capability of the decorated {@code RowAccessible} is preserved. That is, if the given
     * {@code rowAccessible} implements {@link LookaheadRowAccessible}, then the returned {@code RowAccessible} also
     * implements {@code LookaheadRowAccessible}.
     *
     * @param rowAccessible to make uncloseable
     * @return a {@link RowAccessible} that delegates everything to the provided rowAccessible except for the close
     *         method
     */
    public static RowAccessible uncloseable(final RowAccessible rowAccessible) {
        if (rowAccessible instanceof LookaheadRowAccessible) {
            return new UncloseableLookaheadRowAccessible((LookaheadRowAccessible)rowAccessible);
        } else {
            return new UncloseableRowAccessible(rowAccessible);
        }
    }

    private VirtualTableUtils() {

    }

}
