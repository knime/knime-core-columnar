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
 *   Oct 12, 2020 (benjamin): created
 */
package org.knime.core.columnar.access;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.table.access.ListAccess.ListReadAccess;
import org.knime.core.table.access.ListAccess.ListWriteAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ListDataSpec;

/**
 * A ColumnarValueFactory implementation wrapping {@link ListReadData} / {@link ListWriteData} as {@link ListReadAccess}
 * / {@link ListWriteAccess}
 *
 * @param <R> type of the {@link ReadData} for the list elements
 * @param <W> type of the {@link WriteData} for the list elements
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ColumnarListAccessFactory<R extends NullableReadData, // NOSONAR
        W extends NullableWriteData> // NOSONAR
    implements ColumnarAccessFactory {

    private final ColumnarAccessFactory m_innerAccessFactory;

    ColumnarListAccessFactory(final ListDataSpec listAccessSpec) {
        final ColumnarAccessFactory innerAccessFactory =
            ColumnarAccessFactoryMapper.createAccessFactory(listAccessSpec.getInner());
        m_innerAccessFactory = innerAccessFactory;
    }

    @Override
    public ColumnarListWriteAccess<W> createWriteAccess(final ColumnDataIndex index) {
        return new ColumnarListWriteAccess<>(index, m_innerAccessFactory);
    }

    @Override
    public ColumnarListReadAccess<R> createReadAccess(final ColumnDataIndex index) {
        return new ColumnarListReadAccess<>(index, m_innerAccessFactory);
    }

    static final class ColumnarListReadAccess<R extends NullableReadData> extends AbstractReadAccess<ListReadData>
        implements ListReadAccess {

        private int m_lastIndex;

        private int m_innerIndex;

        private R m_innerData;

        private final ColumnarReadAccess m_readAccess;

        ColumnarListReadAccess(final ColumnDataIndex index, final ColumnarAccessFactory innerAccessFactory) {
            super(index);
            m_lastIndex = -1;
            m_readAccess = innerAccessFactory.createReadAccess(() -> m_innerIndex);
        }

        @Override
        public int size() {
            updateInnerData();
            return m_innerData.length();
        }

        @Override
        public <A extends ReadAccess> A getAccess(final int index) { // NOSONAR
            updateInnerData();
            m_innerIndex = index;
            @SuppressWarnings("unchecked")
            final A v = (A)m_readAccess;
            return v;
        }

        private void updateInnerData() {
            final int index = m_index.getIndex();
            // If we got the same index we don't need to create a new access and value
            if (index != m_lastIndex) {
                m_lastIndex = index;
                m_innerData = m_data.createReadData(index);
                m_readAccess.setData(m_innerData);
            }
        }

        @Override
        public void setData(final NullableReadData data) {
            super.setData(data);
            m_lastIndex = -1;
        }

        @Override
        public boolean isMissing(final int index) {
            return m_innerData.isMissing(index);
        }
    }

    static final class ColumnarListWriteAccess<W extends NullableWriteData> extends AbstractWriteAccess<ListWriteData>
        implements ListWriteAccess {

        private final ColumnarWriteAccess m_writeAccess;

        private int m_innerIndex;

        ColumnarListWriteAccess(final ColumnDataIndex index, final ColumnarAccessFactory innerAccessFactory) {
            super(index);
            m_writeAccess = innerAccessFactory.createWriteAccess(() -> m_innerIndex);
        }

        @Override
        public void create(final int size) {
            final W writeData = m_data.createWriteData(m_index.getIndex(), size);
            m_writeAccess.setData(writeData);
        }

        @Override
        public <A extends WriteAccess> A getWriteAccess(final int index) { // NOSONAR
            m_innerIndex = index;
            // NB: m_writeAccess is always the value for the current index
            // because users must call #create at an index first
            @SuppressWarnings("unchecked")
            final A v = (A)m_writeAccess;
            return v;
        }

        @Override
        public void setFromNonMissing(final ReadAccess access) {
            final ListReadAccess listAccess = (ListReadAccess)access;
            final int listSize = listAccess.size();
            create(listSize);
            for (int i = 0; i < listSize; i++) {
                getWriteAccess(i).setFrom(listAccess.getAccess(i));
            }
        }

    }
}
