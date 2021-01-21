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
package org.knime.core.data.columnar.schema;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.ListData.ListDataSpec;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.data.DataValue;
import org.knime.core.data.columnar.ColumnDataIndex;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.access.ListAccess.ListAccessSpec;
import org.knime.core.data.v2.access.ListAccess.ListReadAccess;
import org.knime.core.data.v2.access.ListAccess.ListWriteAccess;
import org.knime.core.data.v2.access.ReadAccess;
import org.knime.core.data.v2.access.WriteAccess;

/**
 * A ColumnarValueFactory implementation wrapping {@link ListReadData} / {@link ListWriteData} as {@link ListReadAccess}
 * / {@link ListWriteAccess}
 *
 * @param <R> type of the {@link ReadData} for the list elements
 * @param <RA> type of the {@link ReadAccess} for the list elements
 * @param <W> type of the {@link WriteData} for the list elements
 * @param <WA> type of the {@link WriteAccess} for the list elements
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ColumnarListAccessFactory<R extends NullableReadData, RA extends ReadAccess, W extends NullableWriteData, WA extends WriteAccess>
    implements ColumnarAccessFactory<ListReadData, ListReadAccess, ListWriteData, ListWriteAccess> {

    private final ValueFactory<RA, WA> m_innerValueFactory;

    private final ColumnarAccessFactory<R, RA, W, WA> m_innerAccessFactory;

    public ColumnarListAccessFactory(final ListAccessSpec<RA, WA> listAccessSpec) {
        m_innerValueFactory = listAccessSpec.getInnerValueFactory();
        @SuppressWarnings("unchecked")
        final ColumnarAccessFactory<R, RA, W, WA> innerAccessFactory =
            (ColumnarAccessFactory<R, RA, W, WA>)listAccessSpec.getInnerSpecs()
                .accept(ColumnarAccessFactoryMapper.INSTANCE);
        m_innerAccessFactory = innerAccessFactory;
    }

    @Override
    public ListWriteAccess createWriteAccess(final ListWriteData data, final ColumnDataIndex index) {
        return new DefaultListWriteAccess(data, index);
    }

    @Override
    public ListReadAccess createReadAccess(final ListReadData data, final ColumnDataIndex index) {
        return new DefaultListReadAccess(data, index);
    }

    @Override
    public DataSpec getColumnDataSpec() {
        return new ListDataSpec(m_innerAccessFactory.getColumnDataSpec());
    }

    private final class DefaultListReadAccess extends AbstractAccess<ListReadData> implements ListReadAccess {

        private int m_lastIndex;

        private int m_innerIndex;

        private ReadValue m_value;

        private R m_innerData;

        public DefaultListReadAccess(final ListReadData data, final ColumnDataIndex index) {
            super(data, index);
            m_lastIndex = -1;
        }

        /** Update the m_value if we are at a new index */
        private void updateReadValue() {
            final int index = m_index.getIndex();
            if (index != m_lastIndex) {
                m_lastIndex = index;
                final R innerData = m_data.createReadData(index);
                // If we got the same object we don't need to create a new access and value
                if (m_innerData != innerData) {
                    m_innerData = innerData;
                    final RA readAccess = m_innerAccessFactory.createReadAccess(m_innerData, () -> m_innerIndex);
                    m_value = m_innerValueFactory.createReadValue(readAccess);
                }
            }
        }

        @Override
        public boolean isMissing() {
            return m_data.isMissing(m_index.getIndex());
        }

        @Override
        public <RV extends ReadValue> RV getReadValue(final int index) {
            updateReadValue();
            m_innerIndex = index;
            @SuppressWarnings("unchecked")
            final RV v = (RV)m_value;
            return v;
        }

        @Override
        public boolean isMissing(final int index) {
            updateReadValue();
            return m_innerData.isMissing(index);
        }

        @Override
        public int size() {
            updateReadValue();
            return m_innerData.length();
        }
    }

    private final class DefaultListWriteAccess extends AbstractAccess<ListWriteData> implements ListWriteAccess {

        private WriteValue<?> m_value;

        private int m_innerIndex;

        public DefaultListWriteAccess(final ListWriteData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public void setMissing() {
            m_data.setMissing(m_index.getIndex());
        }

        @Override
        public <D extends DataValue, WV extends WriteValue<D>> WV getWriteValue(final int index) {
            m_innerIndex = index;
            // NB: m_value is always the value for the current index
            // because users must call #create at an index first
            @SuppressWarnings("unchecked")
            final WV v = (WV)m_value;
            return v;
        }

        @Override
        public void create(final int size) {
            final W writeData = m_data.createWriteData(m_index.getIndex(), size);
            final WA writeAccess = m_innerAccessFactory.createWriteAccess(writeData, () -> m_innerIndex);
            m_value = m_innerValueFactory.createWriteValue(writeAccess);
        }
    }
}
