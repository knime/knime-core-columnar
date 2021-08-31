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
import org.knime.core.columnar.arrow.data.ArrowStringData.ArrowStringDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedStringWriteData;

/**
 * Dictionary encoded string data implementation using Arrow as back end.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowDictEncodedStringData {
    private ArrowDictEncodedStringData() {
    }

    /**
     * Arrow implementation of {@link DictEncodedStringWriteData};
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedStringWriteData extends AbstractArrowDictEncodedWriteData<String>
        implements DictEncodedStringWriteData {

        private ArrowDictEncodedStringWriteData(final ArrowStructWriteData delegate) {
            super(delegate);
        }

        private ArrowDictEncodedStringWriteData(final ArrowStructWriteData delegate, final int offset) {
            super(delegate, offset);
        }

        @Override
        public void setString(final int index, final String val) {
            long dictKey = m_dict.computeIfAbsent(val, v -> {
                ((StringWriteData)m_delegate.getWriteDataAt(AbstractArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                    .setString(index + m_offset, val);
                return generateKey(val);
            });

            setDictKey(index, dictKey);
        }

        @Override
        public ArrowDictEncodedStringWriteData slice(final int start) {
            return new ArrowDictEncodedStringWriteData(m_delegate, start + m_offset);
        }

        @Override
        public ArrowDictEncodedStringReadData close(final int length) {
            return new ArrowDictEncodedStringReadData(m_delegate.close(length));
        }
    }

    /**
     * Arrow implementation of {@link DictEncodedStringReadData};
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedStringReadData extends AbstractArrowDictEncodedReadData<String>
        implements DictEncodedStringReadData {
        private ArrowDictEncodedStringReadData(final ArrowStructReadData delegate) {
            super(delegate);
        }

        private ArrowDictEncodedStringReadData(final ArrowStructReadData delegate, final int[] dictValueLut, final int offset, final int length) {
            super(delegate, dictValueLut, offset, length);
        }

        @Override
        public String getString(final int index) {
            final long dictIndex = getDictKey(index);
            return m_dict.computeIfAbsent(dictIndex,
                i -> ((StringReadData)m_delegate.getReadDataAt(AbstractArrowDictEncodedData.DICT_ENTRY_DATA_INDEX))
                    .getString(m_dictValueLookupTable[index + m_offset]));
        }

        @Override
        public ArrowDictEncodedStringReadData slice(final int start, final int length) {
            return new ArrowDictEncodedStringReadData(m_delegate, m_dictValueLookupTable, start + m_offset, length);
        }
    }

    /**
     * Implementation of {@link ArrowColumnDataFactory} for {@link ArrowDictEncodedStringData}
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ArrowDictEncodedStringDataFactory implements ArrowColumnDataFactory {

        private static final int CURRENT_VERSION = 0;

        /**
         * the single instance of a {@link ArrowDictEncodedStringDataFactory}.
         */
        public static final ArrowDictEncodedStringDataFactory INSTANCE = new ArrowDictEncodedStringDataFactory();

        private final ArrowStructDataFactory m_delegate;

        private ArrowDictEncodedStringDataFactory() {
            m_delegate = new ArrowStructDataFactory(ArrowLongDataFactory.INSTANCE, ArrowStringDataFactory.INSTANCE);
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
            return new ArrowDictEncodedStringWriteData(
                m_delegate.createWrite(vector, dictionaryIdSupplier, allocator, capacity));
        }

        @Override
        public ArrowReadData createRead(final FieldVector vector, final ArrowVectorNullCount nullCount,
            final DictionaryProvider provider, final ArrowColumnDataFactoryVersion version) throws IOException {
            return new ArrowDictEncodedStringReadData(
                m_delegate.createRead(vector, nullCount, provider, version.getChildVersion(0)));
        }

        @Override
        public FieldVector getVector(final NullableReadData data) {
            return m_delegate.getVector(((ArrowDictEncodedStringReadData)data).m_delegate);
        }

        @Override
        public DictionaryProvider getDictionaries(final NullableReadData data) {
            return m_delegate.getDictionaries(((ArrowDictEncodedStringReadData)data).m_delegate);
        }
    }
}
