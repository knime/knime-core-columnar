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
 *   Nov 30, 2020 (benjamin): created
 */
package org.knime.core.columnar.data;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.WriteData;
import org.knime.core.table.schema.StringDataSpec;

/**
 * Class holding {@link StringWriteData}, {@link StringReadData}, and {@link StringDataSpec} for data holding String
 * elements.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public final class StringData {

    private StringData() {
    }

    /**
     * {@link WriteData} holding String elements.
     *
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    public interface StringWriteData extends NullableWriteData {

        /**
         * Assigns a String value to the element at the given index. The contract is that values are only ever set for
         * ascending indices. It is the responsibility of the client calling this method to make sure that the provided
         * index is non-negative and smaller than the capacity of this {@link WriteData}.
         *
         * @param index the index at which to set the String value
         * @param val the String value to set
         */
        void setString(int index, String val);

        @Override
        StringReadData close(int length);

    }

    /**
     * {@link ReadData} holding String elements.
     *
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    public interface StringReadData extends NullableReadData {

        /**
         * Obtains the String value at the given index. It is the responsibility of the client calling this method to
         * make sure that the provided index is non-negative and smaller than the length of this {@link ReadData}.
         *
         * @param index the index at which to obtain the String element
         * @return the String element at the given index
         */
        String getString(int index);

    }

}
