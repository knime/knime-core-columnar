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
 *   Aug 11, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.data.AbstractArrowDictEncodedData.AbstractArrowDictEncodedReadData;
import org.knime.core.columnar.arrow.data.AbstractArrowDictEncodedData.AbstractArrowDictEncodedWriteData;
import org.knime.core.columnar.arrow.data.ArrowLongData.ArrowLongDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructWriteData;
import org.knime.core.columnar.arrow.data.ArrowVarBinaryData.ArrowVarBinaryDataFactory;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedVarBinaryWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Arrow implementation of {@link DictEncodedVarBinaryWriteData} and {@link DictEncodedVarBinaryReadData}
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowDictEncodedVarBinaryData {
    private ArrowDictEncodedVarBinaryData() {
    }

    /**
     * Arrow implementation of {@link DictEncodedVarBinaryWriteData}
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedVarBinaryWriteData extends AbstractArrowDictEncodedWriteData<Object>
        implements DictEncodedVarBinaryWriteData {

        private ArrowDictEncodedVarBinaryWriteData(final ArrowStructWriteData delegate) {
            super(delegate);
        }

        private ArrowDictEncodedVarBinaryWriteData(final ArrowStructWriteData delegate, final int offset) {
            super(delegate, offset);
        }

        @Override
        public <T> void setObject(final int index, final T val, final ObjectSerializer<T> serializer) {
            long dictKey = m_dict.computeIfAbsent(val, v -> {
                ((VarBinaryWriteData)m_delegate.getWriteDataAt(AbstractArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                    .setObject(index + m_offset, val, serializer);
                return generateKey(val);
            });

            setDictKey(index, dictKey);
        }

        @Override
        public ArrowDictEncodedVarBinaryWriteData slice(final int start) {
            return new ArrowDictEncodedVarBinaryWriteData(m_delegate, start + m_offset);
        }

        @Override
        public ArrowDictEncodedVarBinaryReadData close(final int length) {
            return new ArrowDictEncodedVarBinaryReadData(m_delegate.close(length));
        }

        @Override
        public void setBytes(final int index, final byte[] val) {
            long dictKey = m_dict.computeIfAbsent(val, v -> {
                ((VarBinaryWriteData)m_delegate.getWriteDataAt(AbstractArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                    .setBytes(index + m_offset, val);
                return generateKey(val);
            });

            setDictKey(index, dictKey);
        }
    }

    /**
     * Arrow implementation of {@link DictEncodedVarBinaryReadData}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedVarBinaryReadData extends AbstractArrowDictEncodedReadData<Object>
        implements DictEncodedVarBinaryReadData {
        private ArrowDictEncodedVarBinaryReadData(final ArrowStructReadData delegate) {
            super(delegate);
        }

        private ArrowDictEncodedVarBinaryReadData(final ArrowStructReadData delegate, final int[] dictValueLut, final int offset, final int length) {
            super(delegate, dictValueLut, offset, length);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getObject(final int index, final ObjectDeserializer<T> deserializer) {
            final long dictKey = getDictKey(index);
            return (T)m_dict.computeIfAbsent(dictKey,
                i -> ((VarBinaryReadData)m_delegate
                    .getReadDataAt(AbstractArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                    .getObject(m_dictValueLookupTable[index + m_offset], deserializer));
        }

        @Override
        public ArrowDictEncodedVarBinaryReadData slice(final int start, final int length) {
            return new ArrowDictEncodedVarBinaryReadData(m_delegate, m_dictValueLookupTable, start + m_offset, length);
        }

        @Override
        public byte[] getBytes(final int index) {
            final long dictKey = getDictKey(index);
            return (byte[])m_dict.computeIfAbsent(dictKey,
                i -> ((VarBinaryReadData)m_delegate
                    .getReadDataAt(AbstractArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                    .getBytes(m_dictValueLookupTable[index + m_offset]));
        }
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link ArrowDictEncodedVarBinaryData}.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedVarBinaryDataFactory implements ArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        /**
         * the single instance of a {@link ArrowDictEncodedVarBinaryDataFactory}.
         */
        public static final ArrowDictEncodedVarBinaryDataFactory INSTANCE = new ArrowDictEncodedVarBinaryDataFactory();

        private final ArrowStructDataFactory m_delegate;

        private ArrowDictEncodedVarBinaryDataFactory() {
            m_delegate = new ArrowStructDataFactory(ArrowLongDataFactory.INSTANCE, ArrowVarBinaryDataFactory.INSTANCE);
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return ArrowColumnDataFactoryVersion.version(CURRENT_VERSION, m_delegate.m_version);
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return m_delegate.getField(name, dictionaryIdSupplier);
        }

        @Override
        public ArrowWriteData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            return new ArrowDictEncodedVarBinaryWriteData(
                m_delegate.createWrite(vector, dictionaryIdSupplier, allocator, capacity));
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            return new ArrowDictEncodedVarBinaryReadData(m_delegate.createRead(vector, nullCount, provider, version.getChildVersion(0)));
        }

        @Override
        public FieldVector getVector(final NullableReadData data) {
            return m_delegate.getVector(((ArrowDictEncodedVarBinaryReadData)data).m_delegate);
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            return m_delegate.getDictionaries(((ArrowDictEncodedVarBinaryReadData)data).m_delegate);
        }
    }
}
