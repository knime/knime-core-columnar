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
import org.knime.core.table.util.StringEncoder;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OnHeapStringData {

    public static final OnHeapStringDataFactory FACTORY = new OnHeapStringDataFactory();

    private OnHeapStringData() {
    }

    public static final class OnHeapStringWriteData extends AbstractArrowWriteData implements StringWriteData {

        private String[] m_data;

        private OnHeapStringWriteData(final int capacity) {
            super(capacity);
            m_data = new String[capacity];
        }

        private OnHeapStringWriteData(final int offset, final String[] data, final ValidityBuffer validity) {
            super(offset, validity);
            m_data = data;
        }

        @Override
        public void expand(final int minimumCapacity) {
            setNumElements(minimumCapacity);
        }

        @Override
        public int capacity() {
            return m_data.length;
        }

        @Override
        public long usedSizeFor(final int numElements) {
            return numElements * Integer.BYTES + ValidityBuffer.usedSizeFor(numElements);
        }

        @Override
        public long sizeOf() {
            return m_data.length * Integer.BYTES + m_validity.sizeOf();
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new OnHeapStringWriteData(m_offset + start, m_data, m_validity);
        }

        @Override
        public void setString(final int index, final String val) {
            m_data[index + m_offset] = val;
            setValid(index + m_offset);
        }

        @Override
        protected void closeResources() {
            // No resources to close for on-heap data
        }

        @Override
        public OnHeapStringReadData close(final int length) {
            setNumElements(length);
            return new OnHeapStringReadData(m_data, m_validity);
        }

        /**
         * Expand or shrink the data to the given size.
         *
         * @param numElements the new size of the data
         */
        private void setNumElements(final int numElements) {
            m_validity.setNumElements(numElements);

            var newData = new String[numElements];
            System.arraycopy(m_data, 0, newData, 0, Math.min(m_data.length, numElements));
            m_data = newData;
        }
    }

    public static final class OnHeapStringReadData extends AbstractArrowReadData implements StringReadData {

        private final String[] m_data;

        public OnHeapStringReadData(final String[] data, final ValidityBuffer validity) {
            super(validity, data.length);
            m_data = data;
        }

        public OnHeapStringReadData(final String[] data, final ValidityBuffer validity, final int offset,
            final int length) {
            super(validity, offset, length);
            m_data = data;
        }

        @Override
        public long sizeOf() {
            // TODO size of the strings
            return m_data.length * Integer.BYTES + m_validity.sizeOf();
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new OnHeapStringReadData(m_data, m_validity, m_offset + start, length);
        }

        @Override
        public String getString(final int index) {
            return m_data[index + m_offset];
        }
    }

    public static final class OnHeapStringDataFactory extends AbstractArrowColumnDataFactory {

        private OnHeapStringDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public OnHeapStringReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {

            // TODO what is the null count for???

            if (m_version.equals(version)) {

                var decoder = new StringEncoder();
                var valueCount = vector.getValueCount();
                var v = (VarCharVector)vector;

                var data = new String[valueCount];

                for (int i = 0; i < valueCount; i++) {
                    if (!v.isNull(i)) {
                        var val = v.get(i);
                        if (val != null) {
                            data[i] = decoder.decode(val);
                        }
                    }
                }
                var validity = ValidityBuffer.createFrom(vector.getValidityBuffer(), valueCount);

                return new OnHeapStringReadData(data, validity);
            } else {
                throw new IOException(
                    "Cannot read ArrowStringData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public OnHeapStringWriteData createWrite(final int capacity) {
            return new OnHeapStringWriteData(capacity);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.VARCHAR.getType());
        }

        @Override
        public void copyToVector(final NullableReadData data, final FieldVector fieldVector) {
            var d = (OnHeapStringReadData)data; // TODO generic?
            var vector = (VarCharVector)fieldVector;

            vector.allocateNew(d.length());

            // Copy the data
            var encoder = new StringEncoder();
            // TODO we serialize here????? Can this be right???
            for (int i = 0; i < d.length(); i++) {
                if (!d.isMissing(i)) {
                    var val = d.getString(i);
                    if (val != null) {
                        var encoded = encoder.encode(val);
                        vector.setSafe(i, encoded, 0, encoded.limit());
                    } else {
                        vector.setNull(i);
                    }
                } else {
                    vector.setNull(i);
                }
            }

            // Copy the validity
            d.m_validity.copyTo(vector.getValidityBuffer());

            vector.setValueCount(d.length());
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            return null;
        }

        @Override
        public int initialNumBytesPerElement() {
            // TODO
            return 1;
        }
    }
}
