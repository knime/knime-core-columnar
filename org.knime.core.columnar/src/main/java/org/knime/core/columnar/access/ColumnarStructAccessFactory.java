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
package org.knime.core.columnar.access;

import java.util.Arrays;

import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StructAccess.StructReadAccess;
import org.knime.core.table.access.StructAccess.StructWriteAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.DataSpec;

/**
 * A ColumnarValueFactory implementation wrapping {@link StructReadData} / {@link StructWriteData} as
 * {@link StructReadAccess} / {@link StructWriteAccess}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarStructAccessFactory implements ColumnarAccessFactory {

    private final ColumnarAccessFactory[] m_inner;

    /**
     * @param inner the specs of the inner elements
     */
    ColumnarStructAccessFactory(final DataSpec... specs) {
        m_inner = new ColumnarAccessFactory[specs.length];
        for (int i = 0; i < specs.length; i++) {
            m_inner[i] = ColumnarAccessFactoryMapper.createAccessFactory(specs[i]);
        }
    }

    @Override
    public ColumnarStructWriteAccess createWriteAccess(final ColumnDataIndex index) {
        final ColumnarWriteAccess[] inner = Arrays.stream(m_inner)//
            .map(f -> f.createWriteAccess(index))//
            .toArray(ColumnarWriteAccess[]::new);
        return new ColumnarStructWriteAccess(inner, index);
    }

    @Override
    public ColumnarStructReadAccess createReadAccess(final ColumnDataIndex index) {
        return new ColumnarStructReadAccess(m_inner, index);
    }

    static final class ColumnarStructReadAccess extends AbstractReadAccess<StructReadData>
        implements StructReadAccess {

        private final ColumnarReadAccess[] m_inner;

        private ColumnarStructReadAccess(final ColumnarAccessFactory[] inner, final ColumnDataIndex index) {
            super(index);
            m_inner = Arrays.stream(inner)//
                .map(f -> f.createReadAccess(index))//
                .toArray(ColumnarReadAccess[]::new);
        }

        @Override
        public int numInnerReadAccesses() {
            return m_inner.length;
        }

        @Override
        public <R extends ReadAccess> R getInnerReadAccessAt(final int index) {
            @SuppressWarnings("unchecked")
            final R cast = (R)m_inner[index];
            return cast;
        }

        @Override
        public void setData(final NullableReadData data) {
            super.setData(data);
            // super casts the data to the right type for us
            for (int i = 0; i < m_inner.length; i++) {
                m_inner[i].setData(m_data.getReadDataAt(i));
            }
        }

    }

    static final class ColumnarStructWriteAccess extends AbstractWriteAccess<StructWriteData>
        implements StructWriteAccess {

        private ColumnarWriteAccess[] m_inner;

        private ColumnarStructWriteAccess(final ColumnarWriteAccess[] inner, final ColumnDataIndex index) {
            super(index);
            m_inner = inner;
        }

        @Override
        public int numInnerWriteAccesses() {
            return m_inner.length;
        }

        @Override
        public <W extends WriteAccess> W getWriteAccessAt(final int index) {
            @SuppressWarnings("unchecked")
            final W cast = (W)m_inner[index];
            return cast;
        }

        @Override
        public void setData(final NullableWriteData data) {
            super.setData(data);
            // the super class does the cast for us
            for (int i = 0; i < m_inner.length; i++) {
                m_inner[i].setData(m_data.getWriteDataAt(i));
            }
        }

        @Override
        public void setFromNonMissing(final ReadAccess access) {
            final StructReadAccess structAccess = (StructReadAccess)access;
            final int numInnerReadAccesses = structAccess.numInnerReadAccesses();
            for (int i = 0; i < numInnerReadAccesses; i++) {
                m_inner[i].setFrom(structAccess.getInnerReadAccessAt(i));
            }
        }

    }
}
