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
 *   Oct 13, 2020 (benjamin): created
 */
package org.knime.core.columnar.data;

/**
 * Class holding the {@link ListReadData}, {@link ListWriteData} interfaces and the {@link ListDataSpec} class. List
 * data contains a list of elements of a child type at each index.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ListData {

    private ListData() {
    }

    /**
     * A {@link ColumnReadData} which contains a list of elements at each index.
     */
    public static interface ListReadData extends ColumnReadData {

        /**
         * Get the list at the given index. The list itself is a {@link ColumnReadData} again.
         *
         * @param <C> type of the {@link ColumnReadData}
         * @param index the index
         * @return a {@link ColumnReadData} of all the elements of this lists
         */
        <C extends ColumnReadData> C getReadData(int index);
    }

    /**
     * A {@link ColumnWriteData} which contains a list of elements at each index.
     */
    public static interface ListWriteData extends ColumnWriteData {

        /**
         * Get a {@link ColumnWriteData} for a new the list at the given index. Do not call
         * {@link ColumnWriteData#close(int) #close(int)} on the returned data. Only add elements for the indices from 0
         * to (size-1).
         *
         * @param <C> type of the {@link ColumnWriteData}
         * @param index the index in this data object
         * @param size the size of the list
         * @return a {@link ColumnWriteData} to insert elements into the list
         */
        <C extends ColumnWriteData> C getWriteData(int index, int size);

        @Override
        ListReadData close(int length);
    }

    /**
     * The {@link ColumnDataSpec} for list data.
     */
    public static final class ListDataSpec implements ColumnDataSpec {

        private final ColumnDataSpec m_inner;

        /**
         * Create a new spec for list data.
         *
         * @param inner the spec for the elements the lists consist of
         */
        public ListDataSpec(final ColumnDataSpec inner) {
            m_inner = inner;
        }

        /**
         * @return the spec of the elements the lists consist of
         */
        public ColumnDataSpec getInner() {
            return m_inner;
        }

        @Override
        public <R> R accept(final Mapper<R> v) {
            return v.visit(this);
        }
    }
}
