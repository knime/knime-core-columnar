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

import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictKeys.DictKeyGenerator;

/**
 * Interfaces for dictionary encoded read and write data.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DictEncodedData {
    private DictEncodedData() {
    }

    /**
     * Dictionary encoded {@link NullableReadData} that can provide the dictionary key stored at each index inside the
     * data.
     *
     * @param <K> key type, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static interface DictEncodedReadData<K> extends NullableReadData {

        /**
         * Return the key of the dictionary element at the given data index
         *
         * @param dataIndex the index of the element inside this {@link NullableReadData} vector
         * @return the key of the dictionary element, never null
         */
        K getDictKey(final int dataIndex);
    }

    /**
     * Dictionary encoded {@link NullableWriteData} that records a dictionary key at each position instead of the real
     * value. The value for each key is only stored once. The implementation of this dictionary encoding is the
     * responsibility of the back-end.
     *
     * Additionally, a user defined {@link DictKeyGenerator} can be provided which is queried whenever a new value is
     * seen. By default, zero-based ascending long integers are used as keys.
     *
     * @param <K> key type, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static interface DictEncodedWriteData<K> extends NullableWriteData {

        /**
         * Set the key of the dictionary element that is referenced at the given data index
         *
         * @param dataIndex The index into this {@link NullableWriteData} where the dictionary element should be
         *            referenced
         * @param dictKey The key of the dictionary entry
         */
        void setDictKey(final int dataIndex, final K dictKey);

        /**
         * Set a user defined key generator to influence which dictionary key is used when a new value is encountered
         * inside this {@link WriteData}. Could be used to provide the same key as that value had in a previous
         * {@link WriteBatch}.
         *
         * Note: The key generator can only be set once, and only before any values are stored in this
         * {@link WriteData}.
         *
         * @param keyGenerator The key generator will be invoked for each value that has not been seen before.
         */
        void setKeyGenerator(final DictKeyGenerator<K> keyGenerator);
    }

    // ------------------------------------------------------------------------------
    /**
     * Interface specialization for dictionary encoded {@link StringReadData}
     *
     * @param <K> key type, should be Byte, Long or Integer
     */
    public static interface DictEncodedStringReadData<K> extends DictEncodedReadData<K>, StringReadData {

    }

    /**
     * Interface specialization for dictionary encoded {@link StringWriteData}
     *
     * @param <K> key type, should be Byte, Long or Integer
     */
    public static interface DictEncodedStringWriteData<K> extends DictEncodedWriteData<K>, StringWriteData {

    }

    // ------------------------------------------------------------------------------
    /**
     * Interface specialization for dictionary encoded {@link VarBinaryReadData}
     *
     * @param <K> key type, should be Byte, Long or Integer
     */
    public static interface DictEncodedVarBinaryReadData<K> extends DictEncodedReadData<K>, VarBinaryReadData {
    }

    /**
     * Interface specialization for dictionary encoded {@link VarBinaryWriteData}
     *
     * @param <K> key type, should be Byte, Long or Integer
     */
    public static interface DictEncodedVarBinaryWriteData<K> extends DictEncodedWriteData<K>, VarBinaryWriteData {
    }
}
