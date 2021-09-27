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

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructWriteData;
import org.knime.core.columnar.arrow.data.ArrowUnsignedByteData.ArrowUnsignedByteDataFactory;
import org.knime.core.columnar.arrow.data.ArrowUnsignedIntData.ArrowUnsignedIntDataFactory;
import org.knime.core.columnar.arrow.data.ArrowUnsignedLongData.ArrowUnsignedLongDataFactory;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedWriteData;
import org.knime.core.columnar.data.dictencoding.DictKeys;
import org.knime.core.columnar.data.dictencoding.DictKeys.DictKeyGenerator;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DataTraits;

import com.google.common.base.Preconditions;

/**
 * Base implementations for dictionary encoded data using the Arrow back-end
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class AbstractArrowDictEncodedData {
    private AbstractArrowDictEncodedData() {
    }

    static final int DICT_KEY_DATA_INDEX = 0;

    static final int DICT_ENTRY_DATA_INDEX = 1;

    abstract static class AbstractArrowDictEncodedWriteData<T, K> implements DictEncodedWriteData<K>, ArrowWriteData {

        protected final ArrowStructWriteData m_delegate;

        protected final ConcurrentHashMap<T, K> m_dict = new ConcurrentHashMap<>();

        private DictKeyGenerator<K> m_keyGenerator = null;

        protected final KeyType m_keyType;

        protected int m_offset = 0;

        AbstractArrowDictEncodedWriteData(final ArrowStructWriteData delegate, final KeyType keyType) {
            m_delegate = delegate;
            m_keyType = keyType;
        }

        AbstractArrowDictEncodedWriteData(final ArrowStructWriteData delegate, final KeyType keyType, final int offset) {
            m_delegate = delegate;
            m_offset = offset;
            m_keyType = keyType;
        }

        @Override
        public void setMissing(final int index) {
            m_delegate.setMissing(index + m_offset);
        }

        @Override
        public void expand(final int minimumCapacity) {
            m_delegate.expand(minimumCapacity);
        }

        @Override
        public int capacity() {
            return m_delegate.capacity();
        }

        @Override
        public void retain() {
            m_delegate.retain();
        }

        @Override
        public void release() {
            m_delegate.release();
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
        }

        @Override
        public String toString() {
            return "[" + "references=" + m_delegate.getWriteDataAt(DICT_KEY_DATA_INDEX).toString()
                + ", dictionaryEntries=" + m_delegate.getWriteDataAt(DICT_ENTRY_DATA_INDEX).toString() + "]";
        }

        @Override
        public void setDictKey(final int dataIndex, final K dictEntryIndex) {
            final int idx = dataIndex + m_offset;
            final var data = m_delegate.getWriteDataAt(DICT_KEY_DATA_INDEX);

            if (m_keyType == KeyType.BYTE_KEY) {
                ((ByteWriteData)data).setByte(idx, (Byte)dictEntryIndex);
            } else if (m_keyType == KeyType.INT_KEY) {
                ((IntWriteData)data).setInt(idx, (Integer)dictEntryIndex);
            } else {
                ((LongWriteData)data).setLong(idx, (Long)dictEntryIndex);
            }
        }

        @Override
        public void setKeyGenerator(final DictKeyGenerator<K> keyGenerator) {
            if (m_keyGenerator != null) {
                throw new IllegalStateException(
                    "The DictKeyGenerator was already configured before, cannot be set again");
            }

            m_keyGenerator = keyGenerator;
        }

        protected K generateKey(final T value) {
            if (m_keyGenerator == null) {
                m_keyGenerator = DictKeys.createAscendingKeyGenerator(m_keyType);
            }

            return m_keyGenerator.generateKey(value);
        }
    }

    abstract static class AbstractArrowDictEncodedReadData<T, K> implements DictEncodedReadData<K>, ArrowReadData {

        protected final ConcurrentHashMap<K, T> m_dict = new ConcurrentHashMap<>();

        /**
         * A vector of the length of this data, containing the index inside this data where we can look up the value of
         * the referenced dictionary entry.
         *
         * E.g. we have values "A" and "B" stored at indices 0 and 1 respectively, then all following positions in the
         * vector referencing "B" will contain a 1, such that we can immediately find the dict entry value on random
         * access.
         */
        protected final int[] m_dictValueLookupTable;

        protected final ArrowStructReadData m_delegate;

        protected int m_offset = 0;

        private final int m_length;

        protected final KeyType m_keyType;

        AbstractArrowDictEncodedReadData(final ArrowStructReadData delegate, final KeyType keyType) {
            m_delegate = delegate;
            m_length = m_delegate.length();
            m_keyType = keyType;
            m_dictValueLookupTable = constructDictKeyIndexMap();
        }

        AbstractArrowDictEncodedReadData(final ArrowStructReadData delegate, final KeyType keyType, final int[] dictValueLut,
            final int offset, final int length) {
            m_delegate = delegate;
            m_dictValueLookupTable = dictValueLut;
            m_offset = offset;
            m_length = length;
            m_keyType = keyType;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_delegate.isMissing(index + m_offset);
        }

        @Override
        public int length() {
            return m_length;
        }

        @Override
        public void retain() {
            m_delegate.retain();
        }

        @Override
        public void release() {
            m_delegate.release();
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
        }

        @Override
        public String toString() {
            return "[" + "references=" + m_delegate.getReadDataAt(DICT_KEY_DATA_INDEX).toString()
                + ", dictionaryEntries=" + m_delegate.getReadDataAt(DICT_ENTRY_DATA_INDEX).toString() + "]";
        }

        @SuppressWarnings("unchecked")
        @Override
        public K getDictKey(final int dataIndex) {
            final int idx = dataIndex + m_offset;
            final var data = m_delegate.getReadDataAt(DICT_KEY_DATA_INDEX);

            if (m_keyType == KeyType.BYTE_KEY) {
                return (K)Byte.valueOf(((ByteReadData)data).getByte(idx));
            } else if (m_keyType == KeyType.INT_KEY) {
                return (K)Integer.valueOf(((IntReadData)data).getInt(idx));
            } else {
                return (K)Long.valueOf(((LongReadData)data).getLong(idx));
            }
        }

        private int[] constructDictKeyIndexMap() {
            var references = new int[m_delegate.length()];
            var map = new HashMap<K, Integer>();
            for (int i = 0; i < length(); i++) {
                if (isMissing(i)) {
                    continue;
                }
                K key = getDictKey(i);
                final int index = i;
                references[i] = map.computeIfAbsent(key, k -> index);
            }
            return references;
        }
    }

    /**
     * Base implementation for dict encoded {@link ArrowColumnDataFactory}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    abstract static class AbstractArrowDictEncodedDataFactory implements ArrowColumnDataFactory {

        protected final ArrowStructDataFactory m_delegate;

        protected final KeyType m_keyType;

        private final int m_version;

        protected AbstractArrowDictEncodedDataFactory(final DataTraits traits, final ArrowColumnDataFactory valueFactory,
            final int version) {
            Preconditions.checkArgument(DictEncodingTrait.isEnabled(traits), "The column is not dictionary encoded.");
            m_keyType = DictEncodingTrait.keyType(traits);
            // TODO is null correct here, or should this rather create StructDataTraits to delegate any traits down
            m_delegate = new ArrowStructDataFactory(null, createKeyDataFactory(), valueFactory);
            m_version = version;
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return ArrowColumnDataFactoryVersion.version(m_version, m_delegate.m_version);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return m_delegate.getField(name, dictionaryIdSupplier);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public FieldVector getVector(final NullableReadData data) {
            return m_delegate.getVector(((AbstractArrowDictEncodedReadData)data).m_delegate);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            return m_delegate.getDictionaries(((AbstractArrowDictEncodedReadData)data).m_delegate);
        }

        private ArrowColumnDataFactory createKeyDataFactory() {
            if (m_keyType == KeyType.BYTE_KEY) {
                return ArrowUnsignedByteDataFactory.INSTANCE;
            } else if (m_keyType == KeyType.INT_KEY) {
                return ArrowUnsignedIntDataFactory.INSTANCE;
            } else if (m_keyType == KeyType.LONG_KEY) {
                return ArrowUnsignedLongDataFactory.INSTANCE;
            }

            throw new IllegalArgumentException("Unsupported key type " + m_keyType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_version, m_keyType, m_delegate);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            var other = (AbstractArrowDictEncodedDataFactory)obj;
            return m_keyType == other.m_keyType //
                    && m_version == other.m_version
            && m_delegate.equals(other.m_delegate); //
        }


    }
}
