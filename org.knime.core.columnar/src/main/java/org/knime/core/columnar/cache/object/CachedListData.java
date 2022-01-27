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

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.AbstractCachedData.AbstractCachedDataFactory;
import org.knime.core.columnar.cache.object.AbstractCachedNestedData.AbstractCachedLoadingNestedReadData;
import org.knime.core.columnar.cache.object.AbstractCachedNestedData.AbstractNestedCachedWriteData;
import org.knime.core.columnar.cache.object.CachedData.CachedDataFactory;
import org.knime.core.columnar.cache.object.CachedData.CachedLoadingReadData;
import org.knime.core.columnar.cache.object.CachedData.CachedReadData;
import org.knime.core.columnar.cache.object.CachedData.CachedWriteData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;

/**
 * Encapsulates classes related to caching object data within lists.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CachedListData {

    private CachedListData() {

    }

    static final class CachedListDataFactory extends AbstractCachedDataFactory {

        private final CachedDataFactory m_elementFactory;

        CachedListDataFactory(final CachedDataFactory elementFactory, final ExecutorService executor,
            final Set<CachedWriteData> unclosedData) {
            super(executor, unclosedData);
            m_elementFactory = elementFactory;
        }

        @Override
        public CachedListWriteData createCachedData(final NullableWriteData data, final ColumnDataUniqueId id) {
            var listData = (ListWriteData)data;
            return new CachedListWriteData(listData, m_elementFactory, id);
        }

        @Override
        public NullableReadData createReadData(final NullableReadData data, final ColumnDataUniqueId id) {
            return new CachedLoadingListReadData((ListReadData)data, m_elementFactory, id);
        }

    }

    static final class CachedListWriteData extends AbstractNestedCachedWriteData<ListWriteData, ListReadData>
        implements ListWriteData {

        private final CachedDataFactory m_elementFactory;

        private int[] m_elementSizes;

        private final ColumnDataUniqueId m_id;

        private int m_lastIndex = -1;

        CachedListWriteData(final ListWriteData delegate, final CachedDataFactory elementFactory,
            final ColumnDataUniqueId id) {
            super(delegate, new CachedWriteData[delegate.capacity()]);
            m_elementFactory = elementFactory;
            final var capacity = delegate.capacity();
            m_elementSizes = new int[capacity];
            m_id = id;
        }

        @Override
        public void setMissing(final int index) {
            m_children[index] = null;
            m_delegate.setMissing(index);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <C extends NullableWriteData> C createWriteData(final int index, final int size) {
            if (m_lastIndex > -1) {
                // It's not allowed to write at multiple positions in a list in parallel.
                // See ListWriteData#createWriteData contract
                m_children[m_lastIndex].flush();
            }
            var elementData = m_delegate.createWriteData(index, size);
            m_elementSizes[index] = size;
            var out = (CachedWriteData)m_elementFactory.createWriteData(elementData, m_id.getChild(index));
            m_children[index] = out;
            m_lastIndex = index;
            return (C)out;
        }

        @Override
        public void expandCache() {
            final var newCapacity = m_delegate.capacity();
            if (m_children.length < newCapacity) {
                m_children = Arrays.copyOf(m_children, newCapacity);
                m_elementSizes = Arrays.copyOf(m_elementSizes, newCapacity);
            }
        }

        @Override
        public CachedListReadData close(final int length) {
            var readElements = new CachedReadData[length];
            Arrays.setAll(readElements, this::closeIfPresent);
            return new CachedListReadData(length, readElements);
        }

        private CachedReadData closeIfPresent(final int index) {
            var element = m_children[index];
            if (element != null) {
                // FIXME not allowed see ListWriteData#createWriteData contract
                return element.close(m_elementSizes[index]);
            } else {
                return null;
            }
        }

        final class CachedListReadData extends AbstractCachedReadData implements ListReadData {

            private final CachedReadData[] m_readElements;

            CachedListReadData(final int length, final CachedReadData[] readElements) {
                super(length);
                m_readElements = readElements;
            }

            @Override
            public boolean isMissing(final int index) {
                return m_readElements[index] == null;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <C extends NullableReadData> C createReadData(final int index) {
                return (C)m_readElements[index];
            }

            @Override
            public synchronized ListReadData closeWriteDelegate() {
                // sets m_readDelegate
                super.closeWriteDelegate();
                updateElementReadDelegates();
                return m_readDelegate;
            }

            @Override
            public synchronized void setReadDelegate(final NullableReadData readDelegate) {
                // sets m_readDelegate
                super.setReadDelegate(readDelegate);
                updateElementReadDelegates();
            }

            private void updateElementReadDelegates() {
                for (int i = 0; i < m_readElements.length; i++) {//NOSONAR
                    var elementCache = m_readElements[i];
                    if (elementCache != null) {
                        elementCache.setReadDelegate(m_readDelegate.createReadData(i));
                    }
                }
            }

        }

    }

    static final class CachedLoadingListReadData extends AbstractCachedLoadingNestedReadData<ListReadData>
        implements ListReadData {

        private final CachedDataFactory m_elementFactory;

        private final ColumnDataUniqueId m_id;

        CachedLoadingListReadData(final ListReadData delegate, final CachedDataFactory elementFactory,
            final ColumnDataUniqueId id) {
            super(delegate, new CachedLoadingReadData[delegate.length()]);
            m_elementFactory = elementFactory;
            m_id = id;
        }

        @Override
        public <C extends NullableReadData> C createReadData(final int index) {
            var element = m_children[index];
            if (element == null) {
                element = (CachedLoadingReadData)m_elementFactory.createReadData(m_delegate.createReadData(index),
                    m_id.getChild(index));
                m_children[index] = element;
            }
            @SuppressWarnings("unchecked")
            var casted = (C)element;
            return casted;
        }

    }

}
