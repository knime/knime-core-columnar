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
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.LargeBinary;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractArrowReadData.MissingValues;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

import com.google.common.base.Preconditions;

/**
 * Arrow implementation of {@link VarBinaryWriteData} and {@link VarBinaryReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowVarBinaryData {

    /**
     * The initial number of bytes allocated for each element. 32 is a good estimate for UTF-8 encoded Strings and more
     * memory is allocated when needed.
     */
    private static final long INITAL_BYTES_PER_ELEMENT = 32;

    private ArrowVarBinaryData() {
    }

    /** Arrow implementation of {@link VarBinaryWriteData}. */
    public static final class ArrowVarBinaryWriteData extends AbstractArrowWriteData<LargeVarBinaryVector>
        implements VarBinaryWriteData {

        private ArrowVarBinaryWriteData(final LargeVarBinaryVector vector) {
            super(vector);
        }

        private ArrowVarBinaryWriteData(final LargeVarBinaryVector vector, final int offset) {
            super(vector, offset);
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            m_vector.setSafe(m_offset + index, val);
        }

        @Override
        public <T> void setObject(final int index, final T value, final ObjectSerializer<T> serializer) {
            ArrowBufIO.serialize(m_offset + index, value, m_vector, serializer);
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowVarBinaryWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfVariableWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowVarBinaryReadData close(final int length) {
            final LargeVarBinaryVector vector = closeWithLength(length);
            return new ArrowVarBinaryReadData(vector,
                MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link VarBinaryReadData}. */
    public static final class ArrowVarBinaryReadData extends AbstractArrowReadData<LargeVarBinaryVector>
        implements VarBinaryReadData {

        private ArrowVarBinaryReadData(final LargeVarBinaryVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowVarBinaryReadData(final LargeVarBinaryVector vector, final MissingValues missingValues,
            final int offset, final int length) {
            super(vector, missingValues, offset, length);
        }

        @Override
        public byte[] getBytes(final int index) {
            return m_vector.get(m_offset + index);
        }

        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            return ArrowBufIO.deserialize(m_offset + index, m_vector, deserializer);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowVarBinaryReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfVariableWidth(m_vector);
        }
    }

    /**
     * Only used for backwards compatibility of ZonedDateTime data that used to store the zone id as dict encoded var
     * binary.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class ArrowDictEncodedLegacyDateTimeVarBinaryReadData extends AbstractArrowReadData<IntVector>
        implements VarBinaryReadData {

        private final LargeVarBinaryVector m_dict;

        private ArrowDictEncodedLegacyDateTimeVarBinaryReadData(final IntVector vector, final LargeVarBinaryVector dict,
            final MissingValues missingValues) {
            super(vector, missingValues);
            m_dict = dict;
        }

        private ArrowDictEncodedLegacyDateTimeVarBinaryReadData(final IntVector vector, final LargeVarBinaryVector dict,
            final MissingValues missingValues, final int offset, final int length) {
            super(vector, missingValues, offset, length);
            m_dict = dict;
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector) + ArrowSizeUtils.sizeOfVariableWidth(m_dict);
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowDictEncodedLegacyDateTimeVarBinaryReadData(m_vector, m_dict, m_missingValues, m_offset + start, length);
        }

        @Override
        public byte[] getBytes(final int index) {
            return m_dict.get(getDictIdx(index));
        }

        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            return ArrowBufIO.deserialize(getDictIdx(index), m_dict, deserializer);
        }

        private int getDictIdx(final int index) {
            return m_vector.get(m_offset + index);
        }

        @Override
        protected void closeResources() {
            super.closeResources();
            m_dict.close();
        }

    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowVarBinaryData} */
    public static final class ArrowVarBinaryDataFactory extends AbstractArrowColumnDataFactory {

        /**
         * In version 0 we implemented dict encoding in order to load ZonedDateTime data in a backwards compatible way.
         * Newer versions don't support dict encoding because they should use our own struct dict encoding.
         */
        private static final ArrowColumnDataFactoryVersion V0 = ArrowColumnDataFactoryVersion.version(0);

        /**
         * Singleton instance.
         */
        public static final ArrowVarBinaryDataFactory INSTANCE = new ArrowVarBinaryDataFactory();

        private ArrowVarBinaryDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(1));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, LargeBinary.INSTANCE);
        }

        @Override
        public ArrowVarBinaryWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final LargeVarBinaryVector v = (LargeVarBinaryVector)vector;
            v.allocateNew(capacity * INITAL_BYTES_PER_ELEMENT, capacity);
            return new ArrowVarBinaryWriteData(v);
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            final var missingValues = MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount());
            if (V0.equals(version)) {
                // in case of ZonedDateTime data we stored the version id as dict encoded var binary
                var dictionaryEncoding = vector.getField().getDictionary();
                if (dictionaryEncoding != null) {
                    var dict = provider.lookup(dictionaryEncoding.getId());
                    Preconditions.checkArgument(dict.getVectorType().equals(MinorType.LARGEVARBINARY.getType()),
                        "Encountered dictionary vector of type '%s' but expected a LargeVarBinaryVector.");
                    @SuppressWarnings("resource")
                    LargeVarBinaryVector dictVector = (LargeVarBinaryVector)dict.getVector();
                    return new ArrowDictEncodedLegacyDateTimeVarBinaryReadData((IntVector)vector, dictVector,
                        missingValues);
                } else {
                    return new ArrowVarBinaryReadData((LargeVarBinaryVector)vector,
                        missingValues);
                }
            } else if (m_version.equals(version)) {
                return new ArrowVarBinaryReadData((LargeVarBinaryVector)vector,
                    missingValues);
            } else {
                throw new IOException(
                    "Cannot read ArrowVarBinaryData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return (int)INITAL_BYTES_PER_ELEMENT // data buffer
                + BaseLargeVariableWidthVector.OFFSET_WIDTH // offset buffer
                + 1; // validity bit
        }
    }
}
