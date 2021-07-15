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
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedVarBinaryData.DictEncodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedVarBinaryData.DictEncodedVarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Arrow implementation of dictionary encoded data, where dictionary elements can be accessed additional to the
 * referenced indices into the dictionary.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowDictEncodedVarBinaryData {

    private ArrowDictEncodedVarBinaryData() {
    }

    static final int INIT_DICT_BYTE_SIZE = 1024;

    static final int INIT_DICT_NUM_ENTRIES = 20;

    /**
     * Arrow implementation of {@link DictEncodedVarBinaryWriteData}.
     */
    public static final class ArrowDictEncodedVarBinaryWriteData extends AbstractArrowDictEncodedData.ArrowDictEncodedWriteData<LargeVarBinaryVector>
        implements DictEncodedVarBinaryWriteData {
        private ArrowDictEncodedVarBinaryWriteData(final IntVector vector, final LargeVarBinaryVector dict) {
            super(vector, dict);
        }

        private ArrowDictEncodedVarBinaryWriteData(final IntVector vector, final int offset,
            final LargeVarBinaryVector dict) {
            super(vector, offset, dict);
        }

        @Override
        public <T> void setDictEntry(final int dictionaryIndex, final T obj, final ObjectSerializer<T> serializer) {
            while (dictionaryIndex >= m_dictVector.getValueCapacity()) {
                m_dictVector.reallocValidityAndOffsetBuffers();
            }

            if (m_dictVector.isNull(dictionaryIndex)) {
                ArrowBufIO.serialize(dictionaryIndex, obj, m_dictVector, serializer);
                m_largestDictionaryIndex = Math.max(m_largestDictionaryIndex, dictionaryIndex);
            }
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowDictEncodedVarBinaryWriteData(m_vector, m_offset + start, m_dictVector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowDictEncodedVarBinaryReadData close(final int length) {
            final IntVector vector = closeWithLength(length);
            m_dictVector.setValueCount(m_largestDictionaryIndex + 1);

            return new ArrowDictEncodedVarBinaryReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length), m_dictVector);
        }
    }

    /**
     * Arrow implementation of {@link DictEncodedVarBinaryReadData}.
     */
    public static final class ArrowDictEncodedVarBinaryReadData extends AbstractArrowDictEncodedData.ArrowDictEncodedReadData<LargeVarBinaryVector> implements DictEncodedVarBinaryReadData {

        private ArrowDictEncodedVarBinaryReadData(final IntVector vector, final MissingValues missingValues,
            final LargeVarBinaryVector dict) {
            super(vector, missingValues, dict);
        }

        private ArrowDictEncodedVarBinaryReadData(final IntVector vector, final MissingValues missingValues,
            final int offset, final int length, final LargeVarBinaryVector dict) {
            super(vector, missingValues, offset, length, dict);
        }

        @Override
        public <T> T getDictEntry(final int dictionaryIndex, final ObjectDeserializer<T> deserializer) {
            return ArrowBufIO.deserialize(dictionaryIndex, m_dictVector, deserializer);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowDictEncodedVarBinaryReadData(m_vector, m_missingValues, m_offset + start, length, m_dictVector);
        }
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link ArrowDictEncodedVarBinaryData}
     */
    public static final class ArrowDictEncodedVarBinaryDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowDictEncodedVarBinaryDataFactory} */
        public static final ArrowDictEncodedVarBinaryDataFactory INSTANCE = new ArrowDictEncodedVarBinaryDataFactory();

        /**
         *
         */
        private ArrowDictEncodedVarBinaryDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final DictionaryEncoding dictionary = new DictionaryEncoding(dictionaryIdSupplier.getAsLong(), false, null);
            return new Field(name, new FieldType(true, MinorType.INT.getType(), dictionary), null);
        }

        @Override
        @SuppressWarnings("resource") // Vector closed by data object
        public ArrowDictEncodedVarBinaryWriteData createWrite(final FieldVector vector,
            final LongSupplier dictionaryIdSupplier, final BufferAllocator allocator, final int capacity) {
            // Remove the dictionary id for this encoding from the supplier
            dictionaryIdSupplier.getAsLong();
            final LargeVarBinaryVector dict = new LargeVarBinaryVector("Dictionary", allocator);
            dict.allocateNew(INIT_DICT_BYTE_SIZE, INIT_DICT_NUM_ENTRIES);
            final IntVector v = (IntVector)vector;
            v.allocateNew(capacity);
            return new ArrowDictEncodedVarBinaryWriteData(v, dict);
        }

        @Override
        public ArrowDictEncodedVarBinaryReadData createRead(final FieldVector vector,
            final ArrowVectorNullCount nullCount, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                final long dictId = vector.getField().getFieldType().getDictionary().getId();
                @SuppressWarnings("resource") // Dictionary vector closed by data object
                final LargeVarBinaryVector dict = (LargeVarBinaryVector)provider.lookup(dictId).getVector();
                return new ArrowDictEncodedVarBinaryReadData((IntVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()), dict);
            } else {
                throw new IOException("Cannot read ArrowDictEncodedObjectData data with version " + version
                    + ". Current version: " + m_version + ".");
            }
        }

        @Override
        @SuppressWarnings("resource") // Dictionary vector closed by data object
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            final ArrowDictEncodedVarBinaryReadData objData = (ArrowDictEncodedVarBinaryReadData)data;
            final LargeVarBinaryVector vector = objData.getDictionary();
            final Dictionary dictionary = new Dictionary(vector, objData.m_vector.getField().getDictionary());
            return new SingletonDictionaryProvider(dictionary);
        }
    }
}
