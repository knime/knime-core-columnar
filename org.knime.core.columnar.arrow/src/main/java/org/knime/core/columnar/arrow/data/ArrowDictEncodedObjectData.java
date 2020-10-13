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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.SingletonDictionaryProvider;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
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
 * @param <T> type of objects
 */

// TODO do we need IntVector?
public final class ArrowDictEncodedObjectData<T> extends AbstractFieldVectorData<IntVector>
    implements ObjectWriteData<T>, ObjectReadData<T> {

    private static final long INIT_BYTES_PER_ENTRY = 256;

    // TODO Make configurable?
    private static final int INIT_DICT_SIZE = 128;

    private final BiMap<T, Integer> m_inMemDict;

    private final BiMap<Integer, T> m_invInMemDict;

    private final ObjectDataSerializer<T> m_serializer;

    private VarCharVector m_dictionary;

    private int m_runningDictIndex = 0;

    /** Create with dictionary available */
    private ArrowDictEncodedObjectData(final IntVector vector, final VarCharVector dict,
        final ObjectDataSerializer<T> serializer) {
        super(vector);
        m_inMemDict = HashBiMap.create(INIT_DICT_SIZE);
        m_invInMemDict = m_inMemDict.inverse();
        for (int i = 0; i < dict.getValueCount(); i++) {
            final byte[] val = dict.get(i);
            if (val != null) {
                m_invInMemDict.put(i, serializer.deserialize(val));
            }
        }
        m_dictionary = dict;
        m_serializer = serializer;
    }

    @Override
    public T getObject(final int index) {
        return m_invInMemDict.get(m_vector.get(index));
    }

    @Override
    public synchronized void setObject(final int index, final T value) {
        final Integer dictIndex = m_inMemDict.computeIfAbsent(value, k -> m_runningDictIndex++);
        m_vector.set(index, dictIndex);
    }

    VarCharVector getDictionary() {
        // newly created dictionary.
        final int numDistinctValues = m_inMemDict.size();
        final int dictValueCount = m_dictionary.getValueCount();
        if (dictValueCount == 0) {
            // Allocated new memory for all distinct values
            m_dictionary.allocateNew(INIT_BYTES_PER_ENTRY * numDistinctValues, numDistinctValues);
        }
        // Fill the vector with the encoded values
        for (int i = dictValueCount; i < numDistinctValues; i++) {
            m_dictionary.setSafe(i, m_serializer.serialize(m_invInMemDict.get(i)));
        }
        m_dictionary.setValueCount(numDistinctValues);
        return m_dictionary;
    }

    @Override
    public void setMissing(final int index) {
        // TODO we can speed things likely up directly accessing validity buffer
        // BitVectorHelper.unsetBit(m_vector.getValidityBuffer(), index);
        m_vector.setNull(index);
    }

    @Override
    public boolean isMissing(final int index) {
        return m_vector.isNull(index);
    }

    @Override
    public int capacity() {
        return m_vector.getValueCapacity();
    }

    @Override
    public ObjectReadData<T> close(final int length) {
        m_vector.setValueCount(length);
        return this;
    }

    @Override
    public int length() {
        return m_vector.getValueCount();
    }

    @Override
    protected void closeResources() {
        super.closeResources();
        m_dictionary.close();
    }

    @Override
    @SuppressWarnings("resource") // Buffers handled by vector
    public int sizeOf() {
        return (int)(m_vector.getDataBuffer().capacity() // Index vector
            + m_vector.getValidityBuffer().capacity() //
            + m_dictionary.getDataBuffer().capacity() // Dictionary vector
            + m_dictionary.getValidityBuffer().capacity() //
            + m_dictionary.getOffsetBuffer().capacity());
    }

    @Override
    public String toString() {
        final String s = super.toString();
        return new StringBuilder(s) //
            .append(" -> ").append(m_dictionary.toString()) //
            .toString();
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link ArrowDictEncodedObjectData}
     *
     * @param <T> type of object
     */
    public static final class ArrowDictEncodedObjectDataFactory<T> implements ArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        private final ObjectDataSerializer<T> m_serializer;

        /**
         * @param serializer for serialization
         */
        public ArrowDictEncodedObjectDataFactory(final ObjectDataSerializer<T> serializer) {
            m_serializer = serializer;
        }

        @Override
        @SuppressWarnings("resource") // Vector closed by data object
        public ColumnWriteData createWrite(final BufferAllocator allocator, final int capacity) {
            final IntVector vector = new IntVector("Indices",
                new FieldType(false, MinorType.INT.getType(), new DictionaryEncoding(0, false, null)), allocator);
            vector.allocateNew(capacity);
            final VarCharVector dict = new VarCharVector("Dictionary", allocator);
            return new ArrowDictEncodedObjectData<>(vector, dict, m_serializer);
        }

        @Override
        public ColumnReadData createRead(final FieldVector vector, final DictionaryProvider provider, final int version)
            throws IOException {
            if (version == CURRENT_VERSION) {
                final long dictId = vector.getField().getFieldType().getDictionary().getId();
                @SuppressWarnings("resource") // Dictionary vector closed by data object
                final VarCharVector dict = (VarCharVector)provider.lookup(dictId).getVector();
                return new ArrowDictEncodedObjectData<>((IntVector)vector, dict, m_serializer);
            } else {
                throw new IOException("Cannot read ArrowDictEncodedObjectData data with version " + version
                    + ". Current version: " + CURRENT_VERSION + ".");
            }
        }

        @Override
        public FieldVector getVector(final ColumnReadData data) {
            return ((ArrowDictEncodedObjectData<?>)data).m_vector;
        }

        @Override
        @SuppressWarnings("resource") // Dictionary vector closed by data object
        public DictionaryProvider getDictionaries(final ColumnReadData data) {
            @SuppressWarnings("unchecked")
            final ArrowDictEncodedObjectData<T> objData = (ArrowDictEncodedObjectData<T>)data;
            final VarCharVector vector = objData.getDictionary();
            final Dictionary dictionary = new Dictionary(vector, objData.m_vector.getField().getDictionary());
            return new SingletonDictionaryProvider(dictionary);
        }

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((m_serializer == null) ? 0 : m_serializer.hashCode());
            return result;
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
            ArrowDictEncodedObjectDataFactory<?> other = (ArrowDictEncodedObjectDataFactory<?>)obj;
            if (m_serializer == null) {
                if (other.m_serializer != null) {
                    return false;
                }
            } else if (!m_serializer.equals(other.m_serializer)) {
                return false;
            }
            return true;
        }
    }
}
