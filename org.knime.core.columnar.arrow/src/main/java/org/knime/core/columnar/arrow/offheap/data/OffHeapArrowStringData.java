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
 *   Nov 30, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow.offheap.data;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.offheap.OffHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.data.AbstractOffHeapArrowReadData.MissingValues;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.table.util.StringEncoder;

/**
 * Arrow implementation of {@link StringWriteData} and {@link StringReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class OffHeapArrowStringData {

    /**
     * The initial number of bytes allocated for each element. 32 is a good estimate for UTF-8 encoded Strings and more
     * memory is allocated when needed.
     */
    private static final long INITAL_BYTES_PER_ELEMENT = 32;

    private OffHeapArrowStringData() {
    }

    /** Arrow implementation of {@link StringWriteData}. */
    public static final class ArrowStringWriteData extends AbstractOffHeapArrowWriteData<VarCharVector>
        implements StringWriteData {

        private ArrowStringWriteData(final VarCharVector vector) {
            super(vector);
        }

        private ArrowStringWriteData(final VarCharVector vector, final int offset) {
            super(vector, offset);
        }

        @Override
        public void setString(final int index, final String val) {
            final ByteBuffer encoded = StringEncoder.encode(val);
            m_vector.setSafe(m_offset + index, encoded, 0, encoded.limit());
        }

        @Override
        public OffHeapArrowWriteData slice(final int start) {
            return new ArrowStringWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfVariableWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowStringReadData close(final int length) {
            final VarCharVector vector = closeWithLength(length);
            return new ArrowStringReadData(vector, MissingValues.forValidityBuffer(vector.getValidityBuffer(), length));
        }
    }

    /** Arrow implementation of {@link StringReadData}. */
    public static final class ArrowStringReadData extends AbstractOffHeapArrowReadData<VarCharVector>
        implements StringReadData {

        private ArrowStringReadData(final VarCharVector vector, final MissingValues missingValues) {
            super(vector, missingValues);
        }

        private ArrowStringReadData(final VarCharVector vector, final MissingValues missingValues, final int offset,
            final int length) {
            super(vector, missingValues, offset, length);
        }

        @Override
        public String getString(final int index) {
            return StringEncoder.decode(m_vector.get(m_offset + index));
        }

        @Override
        public OffHeapArrowReadData slice(final int start, final int length) {
            return new ArrowStringReadData(m_vector, m_missingValues, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return OffHeapArrowSizeUtils.sizeOfVariableWidth(m_vector);
        }
    }

    /** Implementation of {@link OffHeapArrowColumnDataFactory} for {@link OffHeapArrowStringData} */
    public static final class ArrowStringDataFactory extends AbstractOffHeapArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowStringDataFactory} */
        public static final ArrowStringDataFactory INSTANCE = new ArrowStringDataFactory();

        private ArrowStringDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name) {
            return Field.nullable(name, MinorType.VARCHAR.getType());
        }

        @Override
        public ArrowStringWriteData createWrite(final FieldVector vector, final int capacity) {
            final VarCharVector v = (VarCharVector)vector;
            v.allocateNew(INITAL_BYTES_PER_ELEMENT * capacity, capacity);
            return new ArrowStringWriteData(v);
        }

        @Override
        public ArrowStringReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowStringReadData((VarCharVector)vector,
                    MissingValues.forNullCount(nullCount.getNullCount(), vector.getValueCount()));
            } else {
                throw new IOException(
                    "Cannot read ArrowStringData with version " + version + ". Current version: " + m_version + ".");
            }
        }

        @Override
        public int initialNumBytesPerElement() {
            return (int)INITAL_BYTES_PER_ELEMENT // data buffer
                + BaseVariableWidthVector.OFFSET_WIDTH // offset buffer
                + 1; // validity bit
        }
    }
}
