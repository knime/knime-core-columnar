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

import java.util.HashMap;

import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.arrow.data.ArrowStructData.ArrowStructWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictKeyGenerator;

/**
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class AbstractArrowDictEncodedData {
    private AbstractArrowDictEncodedData() {
    }

    static final int REFERENCE_DATA_INDEX = 0;

    // TODO is the hash used now? Does it take up space (I think it does)
    static final int HASH_DATA_INDEX = 1;

    static final int DICT_ENTRY_DATA_INDEX = 2;

    abstract static class AbstractArrowDictEncodedWriteData<T> implements DictEncodedWriteData, ArrowWriteData {

        private static final class AscendingKeyGenerator implements DictKeyGenerator {
            private int m_nextDictIndex = 0;

            @Override
            public <T> int generateKey(final T value) {
                return m_nextDictIndex++;
            }
        }

        protected final ArrowStructWriteData m_delegate;

        protected final HashMap<T, Integer> m_dict = new HashMap<>();

        private DictKeyGenerator m_keyGenerator = null;

        AbstractArrowDictEncodedWriteData(final ArrowStructWriteData delegate) {
            m_delegate = delegate;
        }

        @Override
        public void setMissing(final int index) {
            m_delegate.setMissing(index);
        }

        @Override
        public void expand(final int minimumCapacity) {
            m_delegate.expand(minimumCapacity);
        }

        @Override
        public int capacity() {
            return m_delegate.capacity();
        }

        @Override
        public void retain() {
            m_delegate.retain();
        }

        @Override
        public void release() {
            m_delegate.release();
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
        }

        @Override
        public void setDictKey(final int dataIndex, final int dictEntryIndex) {
            ((IntWriteData)m_delegate.getWriteDataAt(REFERENCE_DATA_INDEX)).setInt(dataIndex, dictEntryIndex);
        }

        @Override
        public void setKeyGenerator(final DictKeyGenerator keyGenerator) {
            if (m_keyGenerator != null) {
                throw new IllegalStateException(
                    "The DictKeyGenerator was already configured before, cannot be set again");
            }

            m_keyGenerator = keyGenerator;
        }

        protected int generateKey(final T value) {
            if (m_keyGenerator == null) {
                m_keyGenerator = new AscendingKeyGenerator();
            }

            return m_keyGenerator.generateKey(value);
        }
    }

    abstract static class AbstractArrowDictEncodedReadData<T> implements DictEncodedReadData, ArrowReadData {

        protected final HashMap<Integer, T> m_dict = new HashMap<>();

        protected final ArrowStructReadData m_delegate;

        AbstractArrowDictEncodedReadData(final ArrowStructReadData delegate) {
            m_delegate = delegate;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_delegate.isMissing(index);
        }

        @Override
        public int length() {
            return m_delegate.length();
        }

        @Override
        public void retain() {
            m_delegate.retain();
        }

        @Override
        public void release() {
            m_delegate.release();
        }

        @Override
        public long sizeOf() {
            return m_delegate.sizeOf();
        }

        @Override
        public int getDictKey(final int dataIndex) {
            return ((IntReadData)m_delegate.getReadDataAt(REFERENCE_DATA_INDEX)).getInt(dataIndex);
        }

    }
}
