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
 *   Sep 20, 2024 (benjamin): created
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class ArrowStringData5000 {

    interface CopyableStringWriteData extends StringWriteData {
        void setStringBytes(int index, byte[] val);

        void setFrom(CopyableStringReadData sourceData, int sourceIndex, int targetIndex);
    }

    interface CopyableStringReadData extends StringReadData {
        byte[] getStringBytesNullable(int index);
    }

    private ArrowStringData5000() {
    }

    /**
     * Holds the data of a string column in a combination of a string array and a byte array.
     *
     * Note that none of the two representations is guaranteed to be complete to the last set index.
     *
     * The byte[] is complete up to a certain index (the last one that was encoded) ({@link #lastWrittenBytesIndex()})
     * which is tracked by the offsets buffer. The String[] is sparsely filled. However, it is guaranteed to contain all
     * values after the {@link #lastWrittenBytesIndex()}.
     */
    public static final class ArrowStringWriteData extends AbstractArrowWriteData implements CopyableStringWriteData {

        private final LazyEncodedStringList m_data;

        private ArrowStringWriteData(final int capacity) {
            super(capacity);
            m_data = new LazyEncodedStringList(capacity);
        }

        private ArrowStringWriteData(final int offset, final LazyEncodedStringList data, final ValidityBuffer validity) {
            super(offset, validity);
            m_data = data;
        }

        @Override
        public void expand(final int minimumCapacity) {
            setNumElements(minimumCapacity);
        }

        @Override
        public int capacity() {
            return m_data.size();
        }

        @Override
        public long usedSizeFor(final int numElements) {
            // TODO das ist doch quatsch, oder?
            return numElements * Integer.BYTES + ValidityBuffer.usedSizeFor(numElements);
        }

        @Override
        public long sizeOf() {
            // TODO das ist doch quatsch, oder?
            return m_data.size() * Integer.BYTES + m_validity.sizeOf();
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowStringWriteData(m_offset + start, m_data, m_validity);
        }

        @Override
        public void setString(final int index, final String val) {
            m_data.setString(index + m_offset, val);
            setValid(index + m_offset);
        }

        @Override
        protected void closeResources() {
            // No resources to close for on-heap data
        }

        @Override
        public ArrowStringReadData close(final int length) {
            setNumElements(length);
            // note the offsets reflect the current state of the dataBytes (not necessarily all strings of this data)
            return new ArrowStringReadData(m_data, m_validity);
        }

        /**
         * Expand or shrink the data to the given size.
         *
         * @param numElements the new size of the data
         */
        private void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);
            m_data.setNumElements(numElements);
        }

        // LOW LEVEL COPY

        @Override
        public void setStringBytes(final int index, final byte[] val) {
            m_data.setStringBytes(index + m_offset, val);
            setValid(index + m_offset);
        }

        @Override
        public void setFrom(final CopyableStringReadData sourceData, final int sourceIndex, final int targetIndex) {
            if (sourceData instanceof ArrowStringReadData sourceArrowData) {
                if (sourceArrowData.isMissing(sourceIndex)) {
                    setMissing(targetIndex + m_offset);
                } else {
                    m_data.setFrom(sourceArrowData.m_data, sourceIndex, targetIndex + m_offset);
                }
            }
        }
    }

    public static final class ArrowStringReadData extends AbstractArrowReadData implements CopyableStringReadData {

        private final LazyEncodedStringList m_data;

        public ArrowStringReadData(final LazyEncodedStringList data, final ValidityBuffer validity) {
            super(validity, data.size());
            m_data = data;
        }

        public ArrowStringReadData(final LazyEncodedStringList data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(validity, offset, length);
            m_data = data;
        }

        @Override
        public long sizeOf() {
            // TODO size of the strings
            return m_data.size() * Integer.BYTES + m_validity.sizeOf();
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowStringReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public String getString(final int index) {
            // Assuming not missing (the caller has to check)
            return m_data.getString(index + m_offset);
        }

        @Override
        public byte[] getStringBytesNullable(final int index) {
            if (!isMissing(index + m_offset)) {
                return m_data.getStringBytesNullable(index + m_offset);
            } else {
                return null;
            }
        }
    }

    public static final class ArrowStringDataFactory extends AbstractArrowColumnDataFactory {

        public static final ArrowStringDataFactory INSTANCE = new ArrowStringDataFactory();

        private ArrowStringDataFactory() {
            super(0);
        }

        @Override
        public ArrowStringReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            // TODO what is the null count for???

            if (m_version.equals(version)) {

                var valueCount = vector.getValueCount();
                var v = (VarCharVector)vector;
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);
                var data = LazyEncodedStringList.createFrom(vector.getDataBuffer(), vector.getOffsetBuffer(), valueCount);

                return new ArrowStringReadData(data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowStringData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public ArrowStringWriteData createWrite(final int capacity) {
            return new ArrowStringWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.VARCHAR.getType());
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector fieldVector) {
            var d = (ArrowStringReadData)data; // TODO generic?
            var vector = (VarCharVector)fieldVector;

            d.m_data.copyTo(vector);
            d.m_validity.copyTo(vector.getValidityBuffer());

            vector.setLastSet(d.length() - 1);
            vector.setValueCount(d.length());
        }

        @Override
        public int initialNumBytesPerElement() {
            // TODO
            return 1;
        }
    }
}
