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
 *   Oct 9, 2020 (dietzc): created
 */
package org.knime.core.columnar.cache.object;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.ColumnarSchema;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.ObjectData.GenericObjectDataSpec;
import org.knime.core.columnar.data.StringData.StringDataSpec;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.FilteredColumnSelection;

/**
 * Utility class.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class HeapCacheUtils {

    private HeapCacheUtils() {
    }

    /**
     * Get the indices of all StringData and GenericObjectData in the ColumnStoreSchema.
     *
     * @param schema the ColumnStoreSchema
     * @return indices of ObjectData in the schema
     */
    public static final ColumnSelection getObjectDataIndices(final ColumnarSchema schema) {
        final List<Integer> indices = new ArrayList<>();
        final int length = schema.numColumns();
        for (int i = 0; i < length; i++) {
            // NB: We only cache data which are expensive to serialize/deserialize
            // * For Strings we need to do UTF-8 encoding and decoding
            // * For Generic object we need to call a serializer which might be expensive
            final DataSpec columnDataSpec = schema.getSpec(i);
            if (columnDataSpec instanceof StringDataSpec //
                || columnDataSpec instanceof GenericObjectDataSpec) {
                indices.add(i);
            }
        }
        return new FilteredColumnSelection(length, indices.stream().mapToInt(Integer::intValue).toArray());
    }

}
