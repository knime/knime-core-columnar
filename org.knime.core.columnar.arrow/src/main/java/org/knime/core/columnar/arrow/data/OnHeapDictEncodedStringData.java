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
 *   Aug 11, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractOnHeapDictEncodedData.AbstractOnHeapDictEncodedDataFactory;
import org.knime.core.columnar.arrow.data.AbstractOnHeapDictEncodedData.AbstractOnHeapDictEncodedReadData;
import org.knime.core.columnar.arrow.data.AbstractOnHeapDictEncodedData.AbstractOnHeapDictEncodedWriteData;
import org.knime.core.columnar.arrow.data.OnHeapStructData.OnHeapStructReadData;
import org.knime.core.columnar.arrow.data.OnHeapStructData.OnHeapStructWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictKeys.DictKeyGenerator;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DataTraits;

/**
 * Dictionary encoded string data implementation using Arrow as back end.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapDictEncodedStringData {
    private OnHeapDictEncodedStringData() {
    }

    /**
     * Arrow implementation of {@link DictEncodedStringWriteData};
     *
     * @param <K> key type instance, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class OnHeapDictEncodedStringWriteData<K> extends AbstractOnHeapDictEncodedWriteData<String, K>
        implements DictEncodedStringWriteData<K> {

        private OnHeapDictEncodedStringWriteData(final OnHeapStructWriteData delegate, final KeyType keyType) {
            super(delegate, keyType);
        }

        private OnHeapDictEncodedStringWriteData(final OnHeapStructWriteData delegate, final KeyType keyType,
            final int offset, final ConcurrentHashMap<String, K> dict, final DictKeyGenerator<K> generator) {
            super(delegate, keyType, offset, dict, generator);
        }

        @Override
        public void setString(final int index, final String val) {
            K dictKey = m_dict.computeIfAbsent(val, v -> {
                ((StringWriteData)m_delegate.getWriteDataAt(AbstractOnHeapDictEncodedData.DICT_ENTRY_DATA_INDEX))
                    .setString(index + m_offset, val);
                return generateKey(val);
            });

            setDictKey(index, dictKey);
        }

        @Override
        public OnHeapDictEncodedStringWriteData<K> slice(final int start) {
            return new OnHeapDictEncodedStringWriteData<>(m_delegate, m_keyType, start + m_offset, m_dict,
                m_keyGenerator);
        }

        @Override
        public OnHeapDictEncodedStringReadData<K> close(final int length) {
            return new OnHeapDictEncodedStringReadData<>(m_delegate.close(length), m_keyType);
        }
    }

    /**
     * Arrow implementation of {@link DictEncodedStringReadData};
     *
     * @param <K> key type instance, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class OnHeapDictEncodedStringReadData<K> extends AbstractOnHeapDictEncodedReadData<String, K>
        implements DictEncodedStringReadData<K> {
        private OnHeapDictEncodedStringReadData(final OnHeapStructReadData delegate, final KeyType keyType) {
            super(delegate, keyType);
        }

        private OnHeapDictEncodedStringReadData(final OnHeapStructReadData delegate, final KeyType keyType,
            final int[] dictValueLut, final int offset, final int length) {
            super(delegate, keyType, dictValueLut, offset, length);
        }

        @Override
        public String getString(final int index) {
            final K dictIndex = getDictKey(index);
            return m_dict.computeIfAbsent(dictIndex,
                i -> ((StringReadData)m_delegate.getReadDataAt(AbstractOnHeapDictEncodedData.DICT_ENTRY_DATA_INDEX))
                    .getString(m_dictValueLookupTable[index + m_offset]));
        }

        @Override
        public OnHeapDictEncodedStringReadData<K> slice(final int start, final int length) {
            return new OnHeapDictEncodedStringReadData<>(m_delegate, m_keyType, m_dictValueLookupTable,
                start + m_offset, length);
        }
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link OnHeapDictEncodedStringData}
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class OnHeapDictEncodedStringDataFactory extends AbstractOnHeapDictEncodedDataFactory {

        // TODO key type generic? (to remove cast in StructDictEncodingTest)

        private static final int CURRENT_VERSION = 0;

        /**
         * Constructor.
         *
         * @param traits of the data
         * @throws IllegalArgumentException if traits don't contain {@link DictEncodingTrait}
         */
        public OnHeapDictEncodedStringDataFactory(final DataTraits traits) {
            super(traits, OnHeapStringData.FACTORY, CURRENT_VERSION);
        }

        @Override
        public OnHeapDictEncodedStringWriteData<?> createWrite(final int capacity) {
            return new OnHeapDictEncodedStringWriteData<>(m_delegate.createWrite(capacity), m_keyType);
        }

        @Override
        public OnHeapDictEncodedStringReadData<?> createRead(final FieldVector vector,
            final ArrowVectorNullCount nullCount, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            return new OnHeapDictEncodedStringReadData<>(
                m_delegate.createRead(vector, nullCount, provider, version.getChildVersion(0)), m_keyType);
        }
    }
}
