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
 *   Oct 9, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.schema;

import java.util.stream.Stream;

import org.knime.core.columnar.ColumnDataIndex;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.StructData.StructDataSpec;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.data.v2.access.AccessSpec;
import org.knime.core.data.v2.access.ReadAccess;
import org.knime.core.data.v2.access.WriteAccess;
import org.knime.core.data.v2.access.StructAccess.StructReadAccess;
import org.knime.core.data.v2.access.StructAccess.StructWriteAccess;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class ColumnarStructValueFactory
    implements ColumnarValueFactory<StructReadData, StructReadAccess, StructWriteData, StructWriteAccess> {

    private final ColumnarValueFactory<?, ?, ?, ?>[] m_inner;

    private final ColumnDataSpec m_spec;

    /**
     * @param inner the specs of the inner elements
     */
    public ColumnarStructValueFactory(final AccessSpec<?, ?>... inner) {
        // TODO we could do that without streaming in one loop..:
        m_inner = Stream.of(inner).map(spec -> spec.accept(ColumnarValueFactoryMapper.INSTANCE)) //
            .toArray(ColumnarValueFactory<?, ?, ?, ?>[]::new);
        m_spec =
            new StructDataSpec(Stream.of(m_inner).map((i) -> i.getColumnDataSpec()).toArray(ColumnDataSpec[]::new));

    }

    @Override
    public StructWriteAccess createWriteAccess(final StructWriteData data, final ColumnDataIndex index) {
        return new DefaultStructWriteAccess(data, m_inner, index);
    }

    @Override
    public StructReadAccess createReadAccess(final StructReadData data, final ColumnDataIndex index) {
        return new DefaultStructReadAccess(data, m_inner, index);
    }

    @Override
    public ColumnDataSpec getColumnDataSpec() {
        return m_spec;
    }

    private static final class DefaultStructReadAccess implements StructReadAccess {

        private final ColumnDataIndex m_index;

        private final StructReadData m_data;

        private final ColumnarValueFactory<?, ?, ?, ?>[] m_inner;

        private DefaultStructReadAccess(final StructReadData data, final ColumnarValueFactory<?, ?, ?, ?>[] inner,
            final ColumnDataIndex index) {
            m_index = index;
            m_data = data;
            m_inner = inner;
        }

        @Override
        public boolean isMissing() {
            return m_data.isMissing(m_index.getIndex());
        }

        @Override
        public <R extends ReadAccess> R getInnerReadAccessAt(final int index) {
            @SuppressWarnings("unchecked")
            final R cast = (R)m_inner[index].createReadAccess(m_data.getReadDataAt(index), m_index);
            return cast;
        }
    }

    private static final class DefaultStructWriteAccess implements StructWriteAccess {

        private final ColumnDataIndex m_index;

        private ColumnarValueFactory<?, ?, ?, ?>[] m_inner;

        private final StructWriteData m_data;

        private DefaultStructWriteAccess(final StructWriteData data, final ColumnarValueFactory<?, ?, ?, ?>[] inner,
            final ColumnDataIndex index) {
            m_index = index;
            m_data = data;
            m_inner = inner;
        }

        @Override
        public void setMissing() {
            m_data.setMissing(m_index.getIndex());
        }

        @Override
        public <W extends WriteAccess> W getWriteAccessAt(final int index) {
            @SuppressWarnings("unchecked")
            final W cast = (W)m_inner[index].createWriteAccess(m_data.getWriteDataAt(index), m_index);
            return cast;
        }
    }

}
