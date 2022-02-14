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
package org.knime.core.columnar.filter;

import java.util.function.IntFunction;

import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.table.row.Selection;

/**
 * A selection of columns among a total number of columns.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public interface ColumnSelection {

    /**
     * Determines whether a column at a given index is selected.
     *
     * @param index the index of the column
     * @return true if the column is selected, otherwise false
     */
    boolean isSelected(int index);

    /**
     * Obtains the total number of columns (selected or unselected), and, thus the number of valid indices in this
     * selection.
     *
     * @return number of columns (selected or unselected)
     */
    int numColumns();

    /**
     * Create a new {@link ReadBatch} of size equal to the number of columns in this selection. {@link NullableReadData}
     * in the batch will be populated for selected columns by applying the given function.
     *
     * @param indexToReadData a function that returns data for a given index
     * @return a newly created batch
     */
    ReadBatch createBatch(IntFunction<NullableReadData> indexToReadData);

    /**
     * Create a new {@link ColumnSelection} from the columns in the given {@link Selection}.
     *
     * @param selection selected columns and row range
     * @param numColumns the total number of columns (selected or unselected)
     * @return a new {@link ColumnSelection}
     */
    static ColumnSelection fromSelection(final Selection selection, final int numColumns) {
        if (selection.columns().allSelected()) {
            return new DefaultColumnSelection(numColumns);
        } else {
            return new FilteredColumnSelection(numColumns, selection.columns().getSelected(0, numColumns));
        }
    }
}
