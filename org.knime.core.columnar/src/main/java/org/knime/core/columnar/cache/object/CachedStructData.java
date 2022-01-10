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
import java.util.stream.Stream;

import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.AbstractCachedData.AbstractCachedDataFactory;
import org.knime.core.columnar.cache.object.AbstractCachedNestedData.AbstractCachedLoadingNestedReadData;
import org.knime.core.columnar.cache.object.AbstractCachedNestedData.AbstractNestedCachedWriteData;
import org.knime.core.columnar.cache.object.CachedData.CachedDataFactory;
import org.knime.core.columnar.cache.object.CachedData.CachedLoadingReadData;
import org.knime.core.columnar.cache.object.CachedData.CachedReadData;
import org.knime.core.columnar.cache.object.CachedData.CachedWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CachedStructData {

    CachedStructData() {

    }

    static final class CachedStructDataFactory extends AbstractCachedDataFactory {

        private final CachedDataFactory[] m_factories;

        CachedStructDataFactory(final CachedDataFactory[] factories, final ExecutorService executor,
            final Set<CachedWriteData> unclosedData) {
            super(executor, unclosedData);
            m_factories = factories;
        }

        @Override
        public CachedStructWriteData createCachedData(final NullableWriteData data, final ColumnDataUniqueId id) {
            var structData = (StructWriteData)data;
            var inner = new CachedWriteData[m_factories.length];
            Arrays.setAll(inner, i -> m_factories[i].createWriteData(structData.getWriteDataAt(i), id.getChild(i)));
            return new CachedStructWriteData(structData, inner);
        }

        @Override
        public NullableReadData createReadData(final NullableReadData data, final ColumnDataUniqueId id) {
            var structData = (StructReadData)data;
            var datas = new CachedLoadingReadData[m_factories.length];
            Arrays.setAll(datas, i -> m_factories[i].createReadData(structData.getReadDataAt(i), id.getChild(i)));
            return new CachedLoadingStructReadData(structData, datas);
        }

    }

    /**
     * A {@link StructWriteData} implementation that consists of CachedWriteData objects.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class CachedStructWriteData extends AbstractNestedCachedWriteData<StructWriteData, StructReadData>
        implements StructWriteData {

        CachedStructWriteData(final StructWriteData delegate, final CachedWriteData[] innerCaches) {
            super(delegate, innerCaches);
        }

        @Override
        public void setMissing(final int index) {
            for (var data : m_children) {
                data.setMissing(index);
            }
            m_delegate.setMissing(index);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <C extends NullableWriteData> C getWriteDataAt(final int index) {
            return (C)m_children[index];
        }

        @Override
        public CachedStructReadData close(final int length) {
            var inner = Stream.of(m_children)//
                .map(c -> c.close(length))//
                .toArray(CachedReadData[]::new);
            return new CachedStructReadData(length, inner);
        }

        final class CachedStructReadData extends AbstractCachedReadData implements StructReadData {

            private final CachedReadData[] m_innerReadCaches;

            CachedStructReadData(final int length, final CachedReadData[] innerReadCaches) {
                super(length);
                m_innerReadCaches = innerReadCaches;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <C extends NullableReadData> C getReadDataAt(final int index) {
                return (C)m_innerReadCaches[index];
            }

            @Override
            public boolean isMissing(final int index) {
                for (var cache : m_innerReadCaches) {
                    if (!cache.isMissing(index)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public synchronized StructReadData closeWriteDelegate() {
                // sets m_readDelegate
                super.closeWriteDelegate();
                updateInnerReadDelegates();
                return m_readDelegate;
            }

            @Override
            public synchronized void setReadDelegate(final NullableReadData readDelegate) {
                // sets m_readDelegate
                super.setReadDelegate(readDelegate);
                updateInnerReadDelegates();
            }

            private void updateInnerReadDelegates() {
                for (int i = 0; i < m_innerReadCaches.length; i++) {//NOSONAR
                    m_innerReadCaches[i].setReadDelegate(m_readDelegate.getReadDataAt(i));
                }
            }

        }
    }

    /**
     * A {@link StructReadData} consisting of CachedLoadingReadData objects.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    static final class CachedLoadingStructReadData extends AbstractCachedLoadingNestedReadData<StructReadData>
        implements StructReadData {

        CachedLoadingStructReadData(final StructReadData delegate, final CachedLoadingReadData[] innerData) {
            super(delegate, innerData);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <C extends NullableReadData> C getReadDataAt(final int index) {
            return (C)m_children[index];
        }

    }
}
