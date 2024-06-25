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
 *   Dec 17, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.cache.object;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.knime.core.columnar.cache.object.AbstractCachedValueData.AbstractCachedLoadingValueReadData;
import org.knime.core.columnar.cache.object.AbstractCachedValueData.AbstractCachedValueDataFactory;
import org.knime.core.columnar.cache.object.AbstractCachedValueData.AbstractCachedValueWriteData;
import org.knime.core.columnar.cache.object.CachedData.CachedWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StringData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;

/**
 * Contains all classes related to caching string data.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CachedStringData {

    private CachedStringData() {

    }

    /**
     * CachedDataFactory for string data.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class CachedStringDataFactory extends AbstractCachedValueDataFactory<String[]> {

        CachedStringDataFactory(final ExecutorService executor, final Set<CachedWriteData> unclosedData,
            final CacheManager cacheManager, final ExecutorService serializationExecutor,
            final CountUpDownLatch serializationLatch) {
            super(executor, unclosedData, cacheManager, serializationExecutor, serializationLatch);
        }

        @Override
        protected AbstractCachedValueWriteData<?, ?, ?> createCachedData(final NullableWriteData data,
            final Consumer<Object> cache) {
            return new CachedStringWriteData((StringWriteData)data, m_serializationExecutor, m_serializationLatch,
                cache);
        }

        @Override
        public NullableReadData createCachedData(final NullableReadData data, final String[] array) {
            return new CachedStringLoadingReadData((StringReadData)data, array);
        }

        @Override
        protected String[] createArray(final int length) {
            return new String[length];
        }

    }

    /**
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    static final class CachedStringWriteData
        extends AbstractCachedValueWriteData<StringWriteData, StringReadData, String> implements StringWriteData {

        private boolean m_isSequential;

        CachedStringWriteData(final StringWriteData delegate, final ExecutorService executor,
            final CountUpDownLatch serializationLatch, final Consumer<Object> cache) {
            super(delegate, new String[delegate.capacity()], executor, serializationLatch, cache);
            m_isSequential = true;
        }

        @Override
        public void setString(final int index, final String val) {
            m_data[index] = val;
            m_isSequential = false;
            onSet(index);
        }

        @Override
        public void setFrom(final StringReadData readData, final int sourceIndex, final int targetIndex) {
            m_data[sourceIndex] = readData.setAndGet(m_delegate, sourceIndex, targetIndex);
        }

        @Override
        void serializeAt(final int index) {
            m_delegate.setString(index, m_data[index]);
        }

        @Override
        public CachedStringReadData close(final int length) {
            onClose();
            return new CachedStringReadData(length);
        }

        @Override
        public StringReadData closeDelegate(final int length) {
            return m_delegate.close(length);
        }

        final class CachedStringReadData extends AbstractCachedValueReadData implements StringReadData {

            CachedStringReadData(final int length) {
                super(length);
            }

            @Override
            public String getString(final int index) {
                return m_data[index];
            }

            @Override
            public String setAndGet(final StringWriteData delegate, final int sourceIndex, final int targetIndex) {
                delegate.setFrom(m_delegate.close(5), sourceIndex, targetIndex);
//                delegate.setString(sourceIndex, m_data[sourceIndex]);
                return m_data[sourceIndex];
            }

        }
    }

    /**
     * Wrapper around {@link StringData} for in-heap caching.
     *
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @author Marc Bux, KNIME GmbH, Berlin, Germany
     */
    static final class CachedStringLoadingReadData extends AbstractCachedLoadingValueReadData<StringReadData, String>
        implements StringReadData {

        CachedStringLoadingReadData(final StringReadData delegate, final String[] data) {
            super(delegate, data);
        }

        @Override
        public String getString(final int index) {
            if (m_data[index] == null && !m_delegate.isMissing(index)) {
                m_data[index] = m_delegate.getString(index);
            }
            return m_data[index];
        }

        @Override
        public String setAndGet(final StringWriteData delegate, final int sourceIndex, final int targetIndex) {
            delegate.setFrom(m_delegate, sourceIndex, targetIndex);
            return m_data[sourceIndex];
        }

    }

}
