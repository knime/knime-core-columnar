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
 */
package org.knime.core.data.columnar.schema;

import org.knime.core.columnar.data.DataSpec;
import org.knime.core.columnar.data.LongData.LongDataSpec;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.data.DataCell;
import org.knime.core.data.LongValue;
import org.knime.core.data.columnar.ColumnDataIndex;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.v2.access.LongAccess.LongReadAccess;
import org.knime.core.data.v2.access.LongAccess.LongWriteAccess;

/**
 * A ColumnarValueFactory implementation wrapping {@link LongReadData} / {@link LongWriteData} as {@link LongReadAccess}
 * / {@link LongWriteAccess}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
final class ColumnarLongAccessFactory
    implements ColumnarAccessFactory<LongReadData, LongReadAccess, LongWriteData, LongWriteAccess> {

    /** Instance **/
    static final ColumnarLongAccessFactory INSTANCE = new ColumnarLongAccessFactory();

    private ColumnarLongAccessFactory() {
    }

    @Override
    public LongDataSpec getColumnDataSpec() {
        return DataSpec.longSpec();
    }

    @Override
    public LongReadAccess createReadAccess(final LongReadData data, final ColumnDataIndex index) {
        return new DefaultLongReadAccess(data, index);
    }

    @Override
    public LongWriteAccess createWriteAccess(final LongWriteData data, final ColumnDataIndex index) {
        return new DefaultLongWriteAccess(data, index);
    }

    private static final class DefaultLongReadAccess extends AbstractAccess<LongReadData> implements LongReadAccess {

        DefaultLongReadAccess(final LongReadData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public long getLongValue() {
            return m_data.getLong(m_index.getIndex());
        }

        @Override
        public boolean isMissing() {
            return m_data.isMissing(m_index.getIndex());
        }

        @Override
        public double getDoubleValue() {
            return m_data.getLong(m_index.getIndex());
        }

        @Override
        public DataCell getDataCell() {
            return new LongCell(m_data.getLong(m_index.getIndex()));
        }

        @Override
        public double getRealValue() {
            return m_data.getLong(m_index.getIndex());
        }

        @Override
        public double getImaginaryValue() {
            return 0;
        }

        @Override
        public double getMinSupport() {
            return m_data.getLong(m_index.getIndex());
        }

        @Override
        public double getCore() {
            return m_data.getLong(m_index.getIndex());
        }

        @Override
        public double getMaxSupport() {
            return m_data.getLong(m_index.getIndex());
        }

        @Override
        public double getMinCore() {
            return m_data.getLong(m_index.getIndex());
        }

        @Override
        public double getMaxCore() {
            return m_data.getLong(m_index.getIndex());
        }

        @Override
        public double getCenterOfGravity() {
            return m_data.getLong(m_index.getIndex());
        }

    }

    private static final class DefaultLongWriteAccess extends AbstractAccess<LongWriteData> implements LongWriteAccess {

        DefaultLongWriteAccess(final LongWriteData data, final ColumnDataIndex index) {
            super(data, index);
        }

        @Override
        public void setLongValue(final long value) {
            m_data.setLong(m_index.getIndex(), value);
        }

        @Override
        public void setMissing() {
            m_data.setMissing(m_index.getIndex());
        }

        @Override
        public void setValue(final LongValue value) {
            m_data.setLong(m_index.getIndex(), value.getLongValue());
        }

    }

}
