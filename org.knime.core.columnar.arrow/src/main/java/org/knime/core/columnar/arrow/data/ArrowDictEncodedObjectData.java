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
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.SingletonDictionaryProvider;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ObjectData.ObjectDataSerializer;
import org.knime.core.columnar.data.ObjectData.ObjectReadData;
import org.knime.core.columnar.data.ObjectData.ObjectWriteData;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * Arrow implementation of {@link ObjectReadData} and {@link ObjectWriteData} using a dictionary.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowDictEncodedObjectData {

    private ArrowDictEncodedObjectData() {
    }

    private static final long INIT_BYTES_PER_ENTRY = 256;

    // TODO Make configurable?
    private static final int INIT_DICT_SIZE = 128;

    /** Helper class which holds a dictionary in a Map and can write it to a LargeVarBinaryVector */
    private static final class InMemoryDictEncoding<T> {

        private int m_runningDictIndex = 0;

        private final BiMap<T, Integer> m_inMemDict;

        private final BiMap<Integer, T> m_invInMemDict;

        private final ArrowBufIO<T> m_io;

        private final LargeVarBinaryVector m_vector;

        private InMemoryDictEncoding(final LargeVarBinaryVector vector, final ArrowBufIO<T> io) {
            m_vector = vector;
            m_io = io;
            m_inMemDict = HashBiMap.create(INIT_DICT_SIZE);
            m_invInMemDict = m_inMemDict.inverse();
            for (int i = 0; i < vector.getValueCount(); i++) {
                if (vector.isSet(i) != 0) {
                    m_invInMemDict.put(i, m_io.deserialize(i));
                }
            }
        }

        private T get(final int id) {
            return m_invInMemDict.get(id);
        }

        private Integer set(final T value) {
            return m_inMemDict.computeIfAbsent(value, k -> m_runningDictIndex++);
        }

        private LargeVarBinaryVector getDictionaryVector() {
            final int numDistinctValues = m_inMemDict.size();
            final int dictValueCount = m_vector.getValueCount();
            if (dictValueCount == 0) {
                // Allocated new memory for all distinct values
                m_vector.allocateNew(INIT_BYTES_PER_ENTRY * numDistinctValues, numDistinctValues);
            }
            // Fill the vector with the encoded values
            for (int i = dictValueCount; i < numDistinctValues; i++) {
                m_io.serialize(i, m_invInMemDict.get(i));
            }
            m_vector.setValueCount(numDistinctValues);
            return m_vector;
        }
    }

    /**
     * Arrow implementation of {@link ObjectWriteData}.
     *
     * @param <T> type of objects
     */
    public static final class ArrowDictEncodedObjectWriteData<T> extends AbstractArrowWriteData<IntVector>
        implements ObjectWriteData<T> {

        private InMemoryDictEncoding<T> m_dict;

        private ArrowDictEncodedObjectWriteData(final IntVector vector, final LargeVarBinaryVector dict,
            final ArrowBufIO<T> io) {
            super(vector);
            m_dict = new InMemoryDictEncoding<>(dict, io);
        }

        private ArrowDictEncodedObjectWriteData(final IntVector vector, final int offset,
            final InMemoryDictEncoding<T> dict) {
            super(vector, offset);
            m_dict = dict;
        }

        @Override
        public void setObject(final int index, final T obj) {
            m_vector.set(m_offset + index, m_dict.set(obj));
        }

        @Override
        public long sizeOf() {
            return ArrowDictEncodedObjectData.sizeOf(m_vector, m_dict.m_vector);
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowDictEncodedObjectWriteData<T>(m_vector, m_offset + start, m_dict);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowDictEncodedObjectReadData<T> close(final int length) {
            return new ArrowDictEncodedObjectReadData<>(closeWithLength(length), m_dict);
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_dict.m_vector.close();
        }

        @Override
        public String toString() {
            return super.toString() + " -> " + m_dict.m_vector.toString();
        }
    }

    /**
     * Arrow implementation of {@link ObjectReadData}.
     *
     * @param <T> type of objects
     */
    public static final class ArrowDictEncodedObjectReadData<T> extends AbstractArrowReadData<IntVector>
        implements ObjectReadData<T> {

        private final InMemoryDictEncoding<T> m_dict;

        private ArrowDictEncodedObjectReadData(final IntVector vector, final LargeVarBinaryVector dict,
            final ArrowBufIO<T> io) {
            super(vector);
            m_dict = new InMemoryDictEncoding<>(dict, io);
        }

        private ArrowDictEncodedObjectReadData(final IntVector vector, final InMemoryDictEncoding<T> dict) {
            super(vector);
            m_dict = dict;
        }

        private ArrowDictEncodedObjectReadData(final IntVector vector, final int offset, final int length,
            final InMemoryDictEncoding<T> dict) {
            super(vector, offset, length);
            m_dict = dict;
        }

        LargeVarBinaryVector getDictionary() {
            return m_dict.getDictionaryVector();
        }

        @Override
        public T getObject(final int index) {
            return m_dict.get(m_vector.get(m_offset + index));
        }

        @Override
        public long sizeOf() {
            return ArrowDictEncodedObjectData.sizeOf(m_vector, m_dict.m_vector);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowDictEncodedObjectReadData<>(m_vector, m_offset + start, length, m_dict);
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_dict.m_vector.close();
        }

        @Override
        public String toString() {
            return super.toString() + " -> " + m_dict.m_vector.toString();
        }
    }

    private static int sizeOf(final IntVector vector, final LargeVarBinaryVector dict) {
        return ArrowSizeUtils.sizeOfFixedWidth(vector) + ArrowSizeUtils.sizeOfVariableWidth(dict);
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link ArrowDictEncodedObjectData}
     *
     * @param <T> type of object
     */
    public static final class ArrowDictEncodedObjectDataFactory<T> extends AbstractArrowColumnDataFactory {

        private final ObjectDataSerializer<T> m_serializer;

        /**
         * @param serializer for serialization
         */
        public ArrowDictEncodedObjectDataFactory(final ObjectDataSerializer<T> serializer) {
            super(ArrowColumnDataFactoryVersion.version(0));
            m_serializer = serializer;
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final DictionaryEncoding dictionary = new DictionaryEncoding(dictionaryIdSupplier.getAsLong(), false, null);
            return new Field(name, new FieldType(true, MinorType.INT.getType(), dictionary), null);
        }

        @Override
        @SuppressWarnings("resource") // Vector closed by data object
        public ArrowDictEncodedObjectWriteData<T> createWrite(final FieldVector vector,
            final LongSupplier dictionaryIdSupplier, final BufferAllocator allocator, final int capacity) {
            // Remove the dictionary id for this encoding from the supplier
            dictionaryIdSupplier.getAsLong();
            final LargeVarBinaryVector dict = new LargeVarBinaryVector("Dictionary", allocator);
            final IntVector v = (IntVector)vector;
            v.allocateNew(capacity);
            return new ArrowDictEncodedObjectWriteData<>(v, dict, new ArrowBufIO<>(dict, m_serializer));
        }

        @Override
        public ArrowDictEncodedObjectReadData<T> createRead(final FieldVector vector, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                final long dictId = vector.getField().getFieldType().getDictionary().getId();
                @SuppressWarnings("resource") // Dictionary vector closed by data object
                final LargeVarBinaryVector dict = (LargeVarBinaryVector)provider.lookup(dictId).getVector();
                return new ArrowDictEncodedObjectReadData<>((IntVector)vector, dict,
                    new ArrowBufIO<>(dict, m_serializer));
            } else {
                throw new IOException("Cannot read ArrowDictEncodedObjectData data with version " + version
                    + ". Current version: " + m_version + ".");
            }
        }

        @Override
        @SuppressWarnings("resource") // Dictionary vector closed by data object
        public DictionaryProvider getDictionaries(final ColumnReadData data) {
            @SuppressWarnings("unchecked")
            final ArrowDictEncodedObjectReadData<T> objData = (ArrowDictEncodedObjectReadData<T>)data;
            final LargeVarBinaryVector vector = objData.getDictionary();
            final Dictionary dictionary = new Dictionary(vector, objData.m_vector.getField().getDictionary());
            return new SingletonDictionaryProvider(dictionary);
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof ArrowDictEncodedObjectDataFactory)) {
                return false;
            }
            final ArrowDictEncodedObjectDataFactory<?> o = (ArrowDictEncodedObjectDataFactory<?>)obj;
            return Objects.equals(m_serializer, o.m_serializer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_serializer);
        }
    }
}
