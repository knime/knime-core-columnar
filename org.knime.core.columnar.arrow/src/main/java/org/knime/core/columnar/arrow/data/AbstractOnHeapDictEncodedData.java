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
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.OnHeapByteData.OnHeapByteDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapIntData.OnHeapIntDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapLongData.OnHeapLongDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapStructData.OnHeapStructDataFactory;
import org.knime.core.columnar.arrow.data.OnHeapStructData.OnHeapStructReadData;
import org.knime.core.columnar.arrow.data.OnHeapStructData.OnHeapStructWriteData;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedWriteData;
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
public final class AbstractOnHeapDictEncodedData {
    private AbstractOnHeapDictEncodedData() {
    }

    static final int DICT_KEY_DATA_INDEX = 0;

    static final int DICT_ENTRY_DATA_INDEX = 1;

    abstract static class AbstractOnHeapDictEncodedWriteData<T, K> implements DictEncodedWriteData<K>, ArrowWriteData {

        protected final OnHeapStructWriteData m_delegate;

        protected final ConcurrentHashMap<T, K> m_dict;

        protected DictKeyGenerator<K> m_keyGenerator = null;

        protected final KeyType m_keyType;

        protected final int m_offset;

        AbstractOnHeapDictEncodedWriteData(final OnHeapStructWriteData delegate, final KeyType keyType) {
            this(delegate, keyType, 0);
        }

        AbstractOnHeapDictEncodedWriteData(final OnHeapStructWriteData delegate, final KeyType keyType,
            final int offset) {
            this(delegate, keyType, offset, new ConcurrentHashMap<>(), null);
        }

        /**
         * When slicing dictionary encoded data, we need to share the dictionary and the key generator across all
         * slices, so we pass in both.
         *
         * If this constructor is called from the simpler constructors above, then we do not have a key generator yet
         * (because we are constructing the factories) and it needs to be provided using
         * {{@link #setKeyGenerator(DictKeyGenerator)}.
         */
        AbstractOnHeapDictEncodedWriteData(final OnHeapStructWriteData delegate, final KeyType keyType,
            final int offset, final ConcurrentHashMap<T, K> dict, final DictKeyGenerator<K> keyGenerator) {
            m_delegate = delegate;
            m_offset = offset;
            m_keyType = keyType;
            m_dict = dict;
            m_keyGenerator = keyGenerator;
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
        public long usedSizeFor(final int numElements) {
            return m_delegate.usedSizeFor(numElements);
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
            if (keyGenerator == m_keyGenerator) {
                return;
            }

            if (m_keyGenerator != null) {
                throw new IllegalStateException(
                    "The DictKeyGenerator was already configured before, cannot be set again");
            }

            m_keyGenerator = keyGenerator;
        }

        protected K generateKey(final T value) {
            if (m_keyGenerator == null) {
                throw new IllegalStateException("DictKeyGenerator must be configured before writing any data!");
            }

            return m_keyGenerator.generateKey(value);
        }
    }

    abstract static class AbstractOnHeapDictEncodedReadData<T, K> implements DictEncodedReadData<K>, ArrowReadData {

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

        protected final OnHeapStructReadData m_delegate;

        protected int m_offset = 0;

        private final int m_length;

        protected final KeyType m_keyType;

        AbstractOnHeapDictEncodedReadData(final OnHeapStructReadData delegate, final KeyType keyType) {
            m_delegate = delegate;
            m_length = m_delegate.length();
            m_keyType = keyType;
            m_dictValueLookupTable = constructDictKeyIndexMap();
        }

        AbstractOnHeapDictEncodedReadData(final OnHeapStructReadData delegate, final KeyType keyType,
            final int[] dictValueLut, final int offset, final int length) {
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
        public ValidityBuffer getValidityBuffer() {
            return m_delegate.getValidityBuffer();
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
            for (int i = 0; i < length(); i++) {//NOSONAR
                if (isMissing(i)) {
                    continue;
                }
                K key = getDictKey(i);//NOSONAR
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
     * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
     */
    abstract static class AbstractOnHeapDictEncodedDataFactory extends AbstractArrowColumnDataFactory {

        protected final KeyType m_keyType;

        protected AbstractOnHeapDictEncodedDataFactory(final DataTraits traits,
            final ArrowColumnDataFactory valueFactory, final int version) {
            this(keyType(traits), valueFactory, version);
        }

        private AbstractOnHeapDictEncodedDataFactory(final KeyType keyType, final ArrowColumnDataFactory valueFactory,
            final int version) {
            super(version, new OnHeapStructDataFactory(createKeyDataFactory(keyType), valueFactory));
            m_keyType = keyType;
        }

        protected OnHeapStructWriteData createWriteDelegate(final int capacity) {
            return (OnHeapStructWriteData)m_children[0].createWrite(capacity);
        }

        protected OnHeapStructReadData createReadDelegate(final FieldVector vector,
            final ArrowVectorNullCount nullCount, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            return (OnHeapStructReadData)m_children[0].createRead(vector, nullCount, provider,
                version.getChildVersion(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return m_children[0].getField(name, dictionaryIdSupplier);
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector vector) {
            m_children[0].copyToVector(getDelegateData(data), vector);
        }

        @Override
        public int initialNumBytesPerElement() {
            return m_children[0].initialNumBytesPerElement();
        }

        private static KeyType keyType(final DataTraits traits) {
            Preconditions.checkArgument(DictEncodingTrait.isEnabled(traits), "The column is not dictionary encoded.");
            return DictEncodingTrait.keyType(traits);
        }

        private static ArrowColumnDataFactory createKeyDataFactory(final KeyType keyType) {
            // TODO use unsigned vectors (see AbstractArrowDictEncodedData)
            return switch (keyType) {
                case BYTE_KEY -> OnHeapByteDataFactory.INSTANCE_UNSIGNED;
                case INT_KEY -> OnHeapIntDataFactory.INSTANCE_UNSIGNED;
                case LONG_KEY -> OnHeapLongDataFactory.INSTANCE_UNSIGNED;
            };
        }

        private static NullableReadData getDelegateData(final NullableReadData data) {
            return ((AbstractOnHeapDictEncodedReadData<?, ?>)data).m_delegate;
        }
    }
}