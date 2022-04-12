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
 *   Sep 20, 2020 (dietzc): created
 */
package org.knime.core.columnar.data;

import org.knime.core.columnar.WriteData;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.StructDataSpec;

/**
 * Class holding {@link StructWriteData}, {@link StructReadData}, and {@link StructDataSpec} for data holding struct
 * elements. Each struct element contains a number of objects of certain types.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class StructData {

    private StructData() {
    }

    /**
     * A {@link NullableWriteData} holding struct elements.
     */
    public interface StructWriteData extends NullableWriteData {

        /**
         * Obtains a {@link NullableWriteData} of type C at the given index (column) in the struct, which can then be populated
         * with values. The returned data must not be {@link WriteData#close(int) closed} or
         * {@link WriteData#expand(int) expanded}. It is the responsibility of the client calling this method to make
         * sure that the provided index is non-negative and smaller than the size (i.e., number of inner
         * {@link DataSpec ColumnDataSpecs} in the {@link StructDataSpec}) of this struct.
         *
         * @param <C> the type of the {@link NullableWriteData}
         * @param index the index (column) in the struct at which to obtain the data
         * @return the {@link NullableWriteData} at the given index in the struct
         */
        <C extends NullableWriteData> C getWriteDataAt(int index);

        @Override
        StructReadData close(int length);

    }

    /**
     * A {@link NullableReadData} holding struct elements.
     */
    public interface StructReadData extends NullableReadData {

        /**
         * Obtains a {@link NullableReadData} of type C at the given index (column) in the struct, which can then be used to read
         * values. It is the responsibility of the client calling this method to make sure that the provided index is
         * non-negative and smaller than the size (i.e., number of inner {@link DataSpec ColumnDataSpecs} in the
         * {@link StructDataSpec}) of this struct.
         *
         * @param <C> the type of the {@link NullableReadData}
         * @param index the index (column) in the struct at which to obtain the data
         * @return the {@link NullableReadData} at the given index (column) in the struct
         */
        <C extends NullableReadData> C getReadDataAt(int index);

    }

}
