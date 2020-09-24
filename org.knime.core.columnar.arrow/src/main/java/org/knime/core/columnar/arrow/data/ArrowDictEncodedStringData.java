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
import java.nio.ByteBuffer;

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
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * Arrow implementation of {@link StringWriteData} and {@link StringReadData} using a dictionary.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowDictEncodedStringData extends AbstractFieldVectorData<IntVector>
    implements StringWriteData, StringReadData {

    private static final long INIT_BYTES_PER_STRING = 128;

    // TODO Make configurable?
    private static final int INIT_DICT_SIZE = 2048;

    private final StringEncodingHelper m_encodingHelper;

    private final BiMap<String, Integer> m_inMemDict;

    private final BiMap<Integer, String> m_invInMemDict;

    private VarCharVector m_dictionary;

    private int m_runningDictIndex = 0;

    /** Create with dictionary available */
    private ArrowDictEncodedStringData(final IntVector vector, final VarCharVector dict) {
        super(vector);
        m_encodingHelper = new StringEncodingHelper();
        m_inMemDict = HashBiMap.create(INIT_DICT_SIZE);
        m_invInMemDict = m_inMemDict.inverse();
        for (int i = 0; i < dict.getValueCount(); i++) {
            final byte[] val = dict.get(i);
            if (val != null) {
                m_invInMemDict.put(i, m_encodingHelper.decode(val));
            }
        }
        m_dictionary = dict;
    }

    @Override
    public String getString(final int index) {
        return m_invInMemDict.get(m_vector.get(index));
    }

    @Override
    public synchronized void setString(final int index, final String value) {
        final Integer dictIndex = m_inMemDict.computeIfAbsent(value, k -> m_runningDictIndex++);
        m_vector.set(index, dictIndex);
    }

    VarCharVector getDictionary() {
        // newly created dictionary.
        final int numDistinctValues = m_inMemDict.size();
        final int dictValueCount = m_dictionary.getValueCount();
        if (dictValueCount == 0) {
            // Allocated new memory for all distinct values
            m_dictionary.allocateNew(INIT_BYTES_PER_STRING * numDistinctValues, numDistinctValues);
        }
        // Fill the vector with the encoded values
        for (int i = dictValueCount; i < numDistinctValues; i++) {
            final ByteBuffer encode = m_encodingHelper.encode(m_invInMemDict.get(i));
            m_dictionary.setSafe(i, encode.array(), 0, encode.limit());
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
    public StringReadData close(final int length) {
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

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowDictEncodedStringData} */
    public static final class ArrowDictEncodedStringDataFactory implements ArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        /** Singleton instance of {@link ArrowDictEncodedStringDataFactory} */
        public static final ArrowDictEncodedStringDataFactory INSTANCE = new ArrowDictEncodedStringDataFactory();

        private ArrowDictEncodedStringDataFactory() {
            // Singleton
        }

        @Override
        @SuppressWarnings("resource") // Vector closed by data object
        public ColumnWriteData createWrite(final BufferAllocator allocator, final int capacity) {
            final IntVector vector = new IntVector("Indices",
                new FieldType(false, MinorType.INT.getType(), new DictionaryEncoding(0, false, null)), allocator);
            vector.allocateNew(capacity);
            final VarCharVector dict = new VarCharVector("Dictionary", allocator);
            return new ArrowDictEncodedStringData(vector, dict);
        }

        @Override
        public ColumnReadData createRead(final FieldVector vector, final DictionaryProvider provider, final int version)
            throws IOException {
            if (version == CURRENT_VERSION) {
                final long dictId = vector.getField().getFieldType().getDictionary().getId();
                @SuppressWarnings("resource") // Dictionary vector closed by data object
                final VarCharVector dict = (VarCharVector)provider.lookup(dictId).getVector();
                return new ArrowDictEncodedStringData((IntVector)vector, dict);
            } else {
                throw new IOException("Cannot read ArrowDictEncodedStringData data with version " + version
                    + ". Current version: " + CURRENT_VERSION + ".");
            }
        }

        @Override
        public FieldVector getVector(final ColumnReadData data) {
            return ((ArrowDictEncodedStringData)data).m_vector;
        }

        @Override
        @SuppressWarnings("resource") // Dictionary vector closed by data object
        public DictionaryProvider getDictionaries(final ColumnReadData data) {
            final ArrowDictEncodedStringData stringData = (ArrowDictEncodedStringData)data;
            final VarCharVector vector = stringData.getDictionary();
            final Dictionary dictionary = new Dictionary(vector, stringData.m_vector.getField().getDictionary());
            return new SingletonDictionaryProvider(dictionary);
        }

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }
    }
}
