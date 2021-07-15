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
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
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
import org.knime.core.columnar.data.dictencoding.DictEncodedStringData.DictEncodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedStringData.DictEncodedStringWriteData;

/**
 * Arrow implementation of dictionary encoded data, where dictionary elements can be accessed additional to the
 * referenced indices into the dictionary.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowDictEncodedStringData {

    private ArrowDictEncodedStringData() {
    }

    static final int INIT_DICT_BYTE_SIZE = 1024;

    static final int INIT_DICT_NUM_ENTRIES = 20;

    /**
     * Arrow implementation of {@link DictEncodedStringWriteData}.
     */
    public static final class ArrowDictEncodedStringWriteData extends AbstractArrowDictEncodedData.ArrowDictEncodedWriteData<VarCharVector>
        implements DictEncodedStringWriteData {

        private StringEncoder m_encoder = new StringEncoder();

        private ArrowDictEncodedStringWriteData(final IntVector vector, final VarCharVector dict) {
            super(vector, dict);
        }

        private ArrowDictEncodedStringWriteData(final IntVector vector, final int offset,
            final VarCharVector dict) {
            super(vector, offset, dict);
        }

        @Override
        public void setDictEntry(final int dictionaryIndex, final String value) {
            while (dictionaryIndex >= m_dictVector.getValueCapacity()) {
                m_dictVector.reallocValidityAndOffsetBuffers();
            }

            if (m_dictVector.isNull(dictionaryIndex)) {
                final ByteBuffer encoded = m_encoder.encode(value);
                m_dictVector.setSafe(dictionaryIndex, encoded, 0, encoded.limit());
                m_largestDictionaryIndex = Math.max(m_largestDictionaryIndex, dictionaryIndex);
            }
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowDictEncodedStringWriteData(m_vector, m_offset + start, m_dictVector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowDictEncodedStringReadData close(final int length) {
            final IntVector vector = closeWithLength(length);
            m_dictVector.setValueCount(m_largestDictionaryIndex + 1);

            return new ArrowDictEncodedStringReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length), m_dictVector);
        }
    }

    /**
     * Arrow implementation of {@link DictEncodedStringReadData}.
     */
    public static final class ArrowDictEncodedStringReadData extends AbstractArrowDictEncodedData.ArrowDictEncodedReadData<VarCharVector> implements DictEncodedStringReadData {

        private final StringEncoder m_decoder = new StringEncoder();

        private ArrowDictEncodedStringReadData(final IntVector vector, final MissingValues missingValues,
            final VarCharVector dict) {
            super(vector, missingValues, dict);
        }

        private ArrowDictEncodedStringReadData(final IntVector vector, final MissingValues missingValues,
            final int offset, final int length, final VarCharVector dict) {
            super(vector, missingValues, offset, length, dict);
        }

        @Override
        public String getDictEntry(final int dictionaryIndex) {
            return m_decoder.decode(m_dictVector.get(dictionaryIndex));
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowDictEncodedStringReadData(m_vector, m_missingValues, m_offset + start, length, m_dictVector);
        }
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link ArrowDictEncodedStringData}
     */
    public static final class ArrowDictEncodedStringDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowDictEncodedStringDataFactory} */
        public static final ArrowDictEncodedStringDataFactory INSTANCE = new ArrowDictEncodedStringDataFactory();

        /**
         *
         */
        private ArrowDictEncodedStringDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            final DictionaryEncoding dictionary = new DictionaryEncoding(dictionaryIdSupplier.getAsLong(), false, null);
            return new Field(name, new FieldType(true, MinorType.INT.getType(), dictionary), null);
        }

        @Override
        @SuppressWarnings("resource") // Vector closed by data object
        public ArrowDictEncodedStringWriteData createWrite(final FieldVector vector,
            final LongSupplier dictionaryIdSupplier, final BufferAllocator allocator, final int capacity) {
            // Remove the dictionary id for this encoding from the supplier
            dictionaryIdSupplier.getAsLong();
            final VarCharVector dict = new VarCharVector("Dictionary", allocator);
            dict.allocateNew(INIT_DICT_BYTE_SIZE, INIT_DICT_NUM_ENTRIES);
            final IntVector v = (IntVector)vector;
            v.allocateNew(capacity);
            return new ArrowDictEncodedStringWriteData(v, dict);
        }

        @Override
        public ArrowDictEncodedStringReadData createRead(final FieldVector vector,
            final ArrowVectorNullCount nullCount, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                final long dictId = vector.getField().getFieldType().getDictionary().getId();
                @SuppressWarnings("resource") // Dictionary vector closed by data object
                final VarCharVector dict = (VarCharVector)provider.lookup(dictId).getVector();
                return new ArrowDictEncodedStringReadData((IntVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()), dict);
            } else {
                throw new IOException("Cannot read ArrowDictEncodedObjectData data with version " + version
                    + ". Current version: " + m_version + ".");
            }
        }

        @Override
        @SuppressWarnings("resource") // Dictionary vector closed by data object
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            final ArrowDictEncodedStringReadData objData = (ArrowDictEncodedStringReadData)data;
            final VarCharVector vector = objData.getDictionary();
            final Dictionary dictionary = new Dictionary(vector, objData.m_vector.getField().getDictionary());
            return new SingletonDictionaryProvider(dictionary);
        }
    }
}
