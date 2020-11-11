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
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.data.LocalDateData.LocalDateReadData;
import org.knime.core.columnar.data.LocalDateData.LocalDateWriteData;

/**
 * Arrow implementation of {@link LocalDateWriteData} and {@link LocalDateReadData}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowLocalDateData {

    private ArrowLocalDateData() {
    }

    /** Arrow implementation of {@link LocalDateWriteData}. */
    public static final class ArrowLocalDateWriteData extends AbstractArrowWriteData<BigIntVector>
        implements LocalDateWriteData {

        private ArrowLocalDateWriteData(final BigIntVector vector) {
            super(vector);
        }

        private ArrowLocalDateWriteData(final BigIntVector vector, final int offset) {
            super(vector, offset);
        }

        @Override
        public void setLocalDate(final int index, final LocalDate val) {
            m_vector.set(m_offset + index, val.getLong(ChronoField.EPOCH_DAY));
        }

        @Override
        public ArrowWriteData slice(final int start) {
            return new ArrowLocalDateWriteData(m_vector, m_offset + start);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }

        @Override
        @SuppressWarnings("resource") // Resource closed by ReadData
        public ArrowLocalDateReadData close(final int length) {
            return new ArrowLocalDateReadData(closeWithLength(length));
        }
    }

    /** Arrow implementation of {@link LocalDateReadData}. */
    public static final class ArrowLocalDateReadData extends AbstractArrowReadData<BigIntVector>
        implements LocalDateReadData {

        private ArrowLocalDateReadData(final BigIntVector vector) {
            super(vector);
        }

        private ArrowLocalDateReadData(final BigIntVector vector, final int offset, final int length) {
            super(vector, offset, length);
        }

        @Override
        public LocalDate getLocalDate(final int index) {
            return LocalDate.ofEpochDay(m_vector.get(m_offset + index));
        }

        @Override
        public ArrowReadData slice(final int start, final int length) {
            return new ArrowLocalDateReadData(m_vector, m_offset + start, length);
        }

        @Override
        public long sizeOf() {
            return ArrowSizeUtils.sizeOfFixedWidth(m_vector);
        }
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowLocalDateData} */
    public static final class ArrowLocalDateDataFactory extends AbstractArrowColumnDataFactory {

        /** Singleton instance of {@link ArrowLocalDateDataFactory} */
        public static final ArrowLocalDateDataFactory INSTANCE = new ArrowLocalDateDataFactory();

        private ArrowLocalDateDataFactory() {
            super(ArrowColumnDataFactoryVersion.version(0));
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.BIGINT.getType());
        }

        @Override
        public ArrowLocalDateWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final BigIntVector v = (BigIntVector)vector;
            v.allocateNew(capacity);
            return new ArrowLocalDateWriteData(v);
        }

        @Override
        public ArrowLocalDateReadData createRead(final FieldVector vector, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            if (m_version.equals(version)) {
                return new ArrowLocalDateReadData((BigIntVector)vector);
            } else {
                throw new IOException(
                    "Cannot read ArrowLocalDateData with version " + version + ". Current version: " + m_version + ".");
            }
        }
    }
}
