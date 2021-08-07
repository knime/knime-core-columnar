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
 *   Aug 12, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data.dictencoding;

import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;

/**
 * Interfaces for dictionary encoded read and write data.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DictEncodedData {
    private DictEncodedData() {
    }

    /**
     * Dictionary encoded {@link NullableReadData} that can provide the dictionary
     * key stored at each index inside the data.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static interface DictEncodedReadData extends NullableReadData {

        /**
         * Return the key of the dictionary element at the given data index
         *
         * @param dataIndex the index of the element inside this {@link NullableReadData} vector
         * @return the key of the dictionary element
         */
        int getDictKey(final int dataIndex);
    }

    /**
     * Dictionary encoded {@link NullableWriteData} that
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static interface DictEncodedWriteData extends NullableWriteData {

        /**
         * Set the key of the dictionary element that is referenced at the given data index
         *
         * @param dataIndex The index into this {@link NullableWriteData} where the dictionary element should be
         *            referenced
         * @param dictKey The key of the dictionary entry
         */
        void setDictKey(final int dataIndex, final int dictKey);
    }

    // ------------------------------------------------------------------------------
    /**
     * Interface specialization for dictionary encoded {@link StringReadData}
     */
    public static interface DictEncodedStringReadData extends DictEncodedReadData, StringReadData {

    }

    /**
     * Interface specialization for dictionary encoded {@link StringWriteData}
     */
    public static interface DictEncodedStringWriteData extends DictEncodedWriteData, StringWriteData {

    }

    // ------------------------------------------------------------------------------
    /**
     * Interface specialization for dictionary encoded {@link VarBinaryReadData}
     */
    public static interface DictEncodedVarBinaryReadData extends DictEncodedReadData, VarBinaryReadData {
    }

    /**
     * Interface specialization for dictionary encoded {@link VarBinaryWriteData}
     */
    public static interface DictEncodedVarBinaryWriteData extends DictEncodedWriteData, VarBinaryWriteData {
    }
}
