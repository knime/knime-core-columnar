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
package org.knime.core.columnar.arrow.onheap.data;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.onheap.ArrowVectorNullCount;
import org.knime.core.columnar.arrow.onheap.OnHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.onheap.data.AbstractOnHeapArrowDictEncodedData.AbstractArrowDictEncodedDataFactory;
import org.knime.core.columnar.arrow.onheap.data.AbstractOnHeapArrowDictEncodedData.AbstractArrowDictEncodedReadData;
import org.knime.core.columnar.arrow.onheap.data.AbstractOnHeapArrowDictEncodedData.AbstractArrowDictEncodedWriteData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowStructData.ArrowStructWriteData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictKeys.DictKeyGenerator;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;

/**
 * Arrow implementation of {@link DictEncodedVarBinaryWriteData} and {@link DictEncodedVarBinaryReadData}
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class OnHeapArrowDictEncodedVarBinaryData {
    private OnHeapArrowDictEncodedVarBinaryData() {
    }

    /**
     * Arrow implementation of {@link DictEncodedVarBinaryWriteData}
     *
     * @param <K> key type instance, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedVarBinaryWriteData<K> extends AbstractArrowDictEncodedWriteData<Object, K>
        implements DictEncodedVarBinaryWriteData<K> {

        private ArrowDictEncodedVarBinaryWriteData(final ArrowStructWriteData delegate, final KeyType initialDictKey) {
            super(delegate, initialDictKey);
        }

        private ArrowDictEncodedVarBinaryWriteData(final ArrowStructWriteData delegate, final KeyType keyType,
            final int offset, final ConcurrentHashMap<Object, K> dict, final DictKeyGenerator<K> generator) {
            super(delegate, keyType, offset, dict, generator);
        }

        @Override
        public <T> void setObject(final int index, final T val, final ObjectSerializer<T> serializer) {
            K dictKey = m_dict.computeIfAbsent(val, v -> {//NOSONAR
                ((VarBinaryWriteData)m_delegate
                    .getWriteDataAt(AbstractOnHeapArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                        .setObject(index + m_offset, val, serializer);
                return generateKey(val);
            });

            setDictKey(index, dictKey);
        }

        @Override
        public ArrowDictEncodedVarBinaryWriteData<K> slice(final int start) {
            return new ArrowDictEncodedVarBinaryWriteData<>(m_delegate, m_keyType, start + m_offset, m_dict,
                m_keyGenerator);
        }

        @Override
        public ArrowDictEncodedVarBinaryReadData<K> close(final int length) {
            var readData = new ArrowDictEncodedVarBinaryReadData<K>(m_delegate.close(length), m_keyType);
            closeResources();
            return readData;
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            K dictKey = m_dict.computeIfAbsent(val, v -> {//NOSONAR
                ((VarBinaryWriteData)m_delegate
                    .getWriteDataAt(AbstractOnHeapArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                        .setBytes(index + m_offset, val);
                return generateKey(val);
            });

            setDictKey(index, dictKey);
        }
    }

    /**
     * Arrow implementation of {@link DictEncodedVarBinaryReadData}.
     *
     * @param <K> key type instance, should be Byte, Long or Integer
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedVarBinaryReadData<K> extends AbstractArrowDictEncodedReadData<Object, K>
        implements DictEncodedVarBinaryReadData<K> {
        private ArrowDictEncodedVarBinaryReadData(final ArrowStructReadData delegate, final KeyType keyType) {
            super(delegate, keyType);
        }

        private ArrowDictEncodedVarBinaryReadData(final ArrowStructReadData delegate, final KeyType keyType,
            final int[] dictValueLut, final int offset, final int length) {
            super(delegate, keyType, dictValueLut, offset, length);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            final K dictKey = getDictKey(index);//NOSONAR
            return (T)m_dict.computeIfAbsent(dictKey,
                i -> ((VarBinaryReadData)m_delegate
                    .getReadDataAt(AbstractOnHeapArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                        .getObject(m_dictValueLookupTable[index + m_offset], deserializer));
        }

        @Override
        public ArrowDictEncodedVarBinaryReadData<K> slice(final int start, final int length) {
            return new ArrowDictEncodedVarBinaryReadData<>(m_delegate, m_keyType, m_dictValueLookupTable,
                start + m_offset, length);
        }

        @Override
        public byte[] getBytes(final int index) {
            final K dictKey = getDictKey(index);//NOSONAR
            return (byte[])m_dict.computeIfAbsent(dictKey,
                i -> ((VarBinaryReadData)m_delegate
                    .getReadDataAt(AbstractOnHeapArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                        .getBytes(m_dictValueLookupTable[index + m_offset]));
        }
    }

    /**
     * Implementation of {@link OnHeapArrowColumnDataFactory} for {@link OnHeapArrowDictEncodedVarBinaryData}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedVarBinaryDataFactory extends AbstractArrowDictEncodedDataFactory {

        private static final ArrowDictEncodedVarBinaryDataFactory INSTANCE_LONG_KEYS =
            new ArrowDictEncodedVarBinaryDataFactory(KeyType.LONG_KEY);

        private static final ArrowDictEncodedVarBinaryDataFactory INSTANCE_INT_KEYS =
            new ArrowDictEncodedVarBinaryDataFactory(KeyType.INT_KEY);

        private static final ArrowDictEncodedVarBinaryDataFactory INSTANCE_BYTE_KEYS =
            new ArrowDictEncodedVarBinaryDataFactory(KeyType.BYTE_KEY);

        /**
         * Get the instance for the given key type.
         *
         * @param keyType the key type
         * @return the factory instance
         */
        public static ArrowDictEncodedVarBinaryDataFactory getInstance(final KeyType keyType) {
            return switch (keyType) {
                case LONG_KEY -> INSTANCE_LONG_KEYS;
                case INT_KEY -> INSTANCE_INT_KEYS;
                case BYTE_KEY -> INSTANCE_BYTE_KEYS;
            };
        }

        private static final int CURRENT_VERSION = 0;

        private ArrowDictEncodedVarBinaryDataFactory(final KeyType keyType) {
            super(keyType, ArrowVarBinaryDataFactory.INSTANCE, CURRENT_VERSION);
        }

        @Override
        public OnHeapArrowWriteData createWrite(final int capacity) {
            return new ArrowDictEncodedVarBinaryWriteData<>(createWriteDelegate(capacity), m_keyType);
        }

        @Override
        public OnHeapArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (version.getVersion() == CURRENT_VERSION) {
                return new ArrowDictEncodedVarBinaryReadData<>(createReadDelegate(vector, nullCount, provider, version),
                    m_keyType);
            } else {
                throw new IOException("Cannot read ArrowDictEncodedVarBinaryData with version " + version
                    + ". Current version: " + CURRENT_VERSION + ".");
            }
        }
    }
}
