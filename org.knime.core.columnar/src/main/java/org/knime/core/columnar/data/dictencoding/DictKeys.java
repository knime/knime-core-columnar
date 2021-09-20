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
 *   Sep 17, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data.dictencoding;

import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.batch.Batch;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

/**
 * Class containing helpers for dictionary key generation
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DictKeys {
    private DictKeys() {
    }

    /**
     * Class that generates a new dictionary key for a value that was never seen before in the {@link WriteData} of this
     * batch, but possibly in previous batches.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     *
     * @param <K> key type, should be Byte, Long or Integer
     */
    public static interface DictKeyGenerator<K> {

        /**
         * Generate a new key for a given value. Should only be called if the value has never been seen before in the
         * current {@link WriteData}.
         *
         * The generated keys should start at zero and provide ascending keys for newly seen values. If the given value
         * was seen in a previous {@link Batch}, the key of the previous batch should be returned.
         *
         * @param <T> Type of the value
         * @param value The value for which to generate the new key
         * @return newly generated key
         * @throws IllegalStateException if no unique key can be created any more using the current key type
         */
        <T> K generateKey(final T value);
    }

    /**
     * Simple key generator that returns ascending bytes on each call to {@link DictKeyGenerator#generateKey(Object)}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    private static class AscendingByteKeyGenerator implements DictKeyGenerator<Byte> {

        private byte m_nextDictKey = 0;

        private boolean m_isFirstKey = true;

        @Override
        public <T> Byte generateKey(final T value) {
            // Due to Java's overflow strategy, increasing the dictKey will wrap around at MAX_BYTE.
            // To make sure we do not hand out a key duplicate with value 0, we remember whether
            // this is the first key or not.
            if (m_nextDictKey == 0 && !m_isFirstKey) {
                throw new IllegalStateException("No more unused keys available!");
            }

            m_isFirstKey = false;
            return m_nextDictKey++;
        }
    }

    /**
     * Simple key generator that returns ascending integers on each call to
     * {@link DictKeyGenerator#generateKey(Object)}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    private static class AscendingIntKeyGenerator implements DictKeyGenerator<Integer> {

        private int m_nextDictKey = 0;

        private boolean m_isFirstKey = true;

        @Override
        public <T> Integer generateKey(final T value) {
            // Due to Java's overflow strategy, increasing the dictKey will wrap around at MAX_INT.
            // To make sure we do not hand out a key duplicate with value 0, we remember whether
            // this is the first key or not.
            if (m_nextDictKey == 0 && !m_isFirstKey) {
                throw new IllegalStateException("No more unused keys available!");
            }

            m_isFirstKey = false;
            return m_nextDictKey++;
        }
    }

    /**
     * Simple key generator that returns ascending long integers on each call to
     * {@link DictKeyGenerator#generateKey(Object)}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    private static class AscendingLongKeyGenerator implements DictKeyGenerator<Long> {

        private long m_nextDictKey = 0;

        private boolean m_isFirstKey = true;

        @Override
        public <T> Long generateKey(final T value) {
            // Due to Java's overflow strategy, increasing the dictKey will wrap around at MAX_LONG.
            // To make sure we do not hand out a key duplicate with value 0, we remember whether
            // this is the first key or not.
            if (m_nextDictKey == 0 && !m_isFirstKey) {
                throw new IllegalStateException("No more unused keys available!");
            }

            m_isFirstKey = false;
            return m_nextDictKey++;
        }
    }

    /**
     * Create a Key generator that generates ascending keys of integral numbers
     *
     * @param <K> The type of keys, must be Byte, Integer or Long
     * @param keyValue A value of the key type, used to select the appropriate generator
     * @return A new {@link DictKeyGenerator}
     */
    @SuppressWarnings("unchecked")
    public static <K> DictKeyGenerator<K> createAscendingKeyGenerator(final K keyValue) {
        if (null == keyValue) {
            throw new IllegalArgumentException("Cannot determine key data type from null");
        }

        if (keyValue instanceof Byte) {
            return (DictKeyGenerator<K>)new AscendingByteKeyGenerator();
        } else if (keyValue instanceof Integer) {
            return (DictKeyGenerator<K>)new AscendingIntKeyGenerator();
        } else if (keyValue instanceof Long) {
            return (DictKeyGenerator<K>)new AscendingLongKeyGenerator();
        } else {
            throw new IllegalArgumentException(
                "Can only create key generators for Byte, Int and Long, not " + keyValue.getClass().getName());
        }
    }

    /**
     * Create a Key generator that generates ascending keys of integral numbers
     *
     * @param <K> The type of keys, must be Byte, Integer or Long
     * @param keyType The key type to use
     * @return A new {@link DictKeyGenerator}
     */
    @SuppressWarnings("unchecked")
    public static <K> DictKeyGenerator<K> createAscendingKeyGenerator(final KeyType keyType) {
        if (null == keyType) {
            throw new IllegalArgumentException("Cannot determine key data type from null");
        }

        if (keyType == KeyType.BYTE_KEY) {
            return (DictKeyGenerator<K>)new AscendingByteKeyGenerator();
        } else if (keyType == KeyType.INT_KEY) {
            return (DictKeyGenerator<K>)new AscendingIntKeyGenerator();
        } else if (keyType == KeyType.LONG_KEY) {
            return (DictKeyGenerator<K>)new AscendingLongKeyGenerator();
        } else {
            throw new IllegalArgumentException(
                "Can only create key generators for Byte, Int and Long, not " + keyType.getClass().getName());
        }
    }
}
